# Databricks notebook source
# MAGIC %md
# MAGIC # Capstone 31 - Cell Tower Health & Dropped Calls
# MAGIC My topic for the capstone. Full pipeline below: generate data -> bronze -> silver -> gold -> export.
# MAGIC Followed the brief's exact generator code so my row counts line up with the checklist.

# COMMAND ----------

MY_ID = "avishee19"
VOL = f"/Volumes/workspace/capstone_{avishee19}/raw"
SCHEMA = f"workspace.capstone_{avishee19}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

# COMMAND ----------

# MAGIC %md ## Step 1 - generate the data
# MAGIC Not touching the seed/row math here, checklist numbers depend on it being exact.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window

CI = "element_at(array('Pune','Kochi','Indore','Jaipur','Nagpur','Surat'),cast(id%6 as int)+1)"
PK, MD = '(9,10,11,12,17,18,19,20)', '(6,7,8,13,14,15,16,21)'

t = spark.range(120).selectExpr('id n', "format_string('T%03d',id+1) tower_id",
    f"concat({CI},' Site ',format_string('%03d',id+1)) site_name", f'{CI} city',
    "format_string('D%d',cast(id%4 as int)+1) district", 'if(id<80,10,2) capacity_channels',
    "date_add(date'2019-01-01',cast(id*11 as int)) commissioned_on")
t.drop('n').write.mode('overwrite').option('header', True).csv(f'{VOL}/towers')

c = (t.withColumn('call_date', F.explode(F.expr(
        "sequence(date'2025-06-01',date'2025-06-15')")))
      .withColumn('hour_of_day', F.explode(F.expr('sequence(0,23)')))
      .selectExpr('n', 'tower_id', 'call_date', 'hour_of_day', 'if(n<80,6,1) reps',
        f'case when hour_of_day in {PK} then 3 when hour_of_day in {MD} then 2'
        ' else 1 end w')).selectExpr('*', 'w*reps cnt',
        'if(n<80,element_at(array(360,180,72),w),element_at(array(20,10,6),w)) md')
c = (c.withColumn('k', F.explode(F.expr('sequence(1,cnt)'))).selectExpr('*',
        'hour_of_day*3600+(k-1)*cast(3600/cnt as int) ss',
        'element_at(array(45,240,420),w)+(k%4)*element_at(array(15,120,120),w) dur',
        "concat_ws('-',tower_id,string(call_date),string(hour_of_day),string(k)) call_id")
      .withColumn('rb', F.row_number().over(Window.partitionBy('tower_id', 'w')
        .orderBy('call_date', 'hour_of_day', 'k'))))
c = c.selectExpr('*', "case when rb%md=0 then 'DROPPED' when rb%17=1 then 'BUSY'"
    " when rb%23=2 then 'NO_ANSWER' else 'NORMAL' end cause").withColumn(
    'rn', F.row_number().over(Window.orderBy('call_id')))
raw = c.selectExpr('rn', 'call_id',
    "format_string('SUB%05d',cast(pmod(k*37+hour_of_day*11+n*7,60000) as int)) subscriber_id",
    "if(rn%624=2,'T999',tower_id) tower_id",
    "cast(timestampadd(second,ss,cast(call_date as timestamp)) as string) start_ts",
    "case when rn%312=0 then 'NA' when rn%832=3 then '-1' else string(dur) end duration_seconds",
    "if(rn%416=1,concat(' ',lower(cause),' '),cause) end_cause",
    "if(k%2=0,'MT','MO') direction", "if(n%3=0,'5G','4G') technology")
(raw.unionByName(raw.filter('rn%208=5')).drop('rn').write.mode('overwrite')
 .option('header', True).option('ignoreLeadingWhiteSpace', False)
 .option('ignoreTrailingWhiteSpace', False).csv(f'{VOL}/calls'))

print("data generated")

# COMMAND ----------

# MAGIC %md ## Step 2 - bronze
# MAGIC Just landing both files as-is, everything as string, plus some provenance columns.
# MAGIC Note: had to use `_metadata.file_path` instead of `input_file_name()` here - the latter
# MAGIC isn't allowed under Unity Catalog on Free Edition, throws a UC_COMMAND_NOT_SUPPORTED error.

# COMMAND ----------

bronze_towers = (spark.read.option('header', True).csv(f'{VOL}/towers')
    .selectExpr(*[f"cast({c} as string) as {c}" for c in
        ["tower_id", "site_name", "city", "district", "capacity_channels", "commissioned_on"]],
        "_metadata.file_path as _source_file")
    .withColumn('_ingested_at', F.current_timestamp())
    .withColumn('_row_hash', F.sha2(F.concat_ws('||', 'tower_id', 'site_name', 'city',
        'district', 'capacity_channels', 'commissioned_on'), 256)))

bronze_calls = (spark.read.option('header', True).option('ignoreLeadingWhiteSpace', False)
    .option('ignoreTrailingWhiteSpace', False).csv(f'{VOL}/calls')
    .selectExpr(*[f"cast({c} as string) as {c}" for c in
        ["call_id", "subscriber_id", "tower_id", "start_ts", "duration_seconds",
         "end_cause", "direction", "technology"]],
        "_metadata.file_path as _source_file")
    .withColumn('_ingested_at', F.current_timestamp())
    .withColumn('_row_hash', F.sha2(F.concat_ws('||', 'call_id', 'subscriber_id', 'tower_id',
        'start_ts', 'duration_seconds', 'end_cause', 'direction', 'technology'), 256)))

(bronze_towers.write.mode('overwrite').saveAsTable(f'{SCHEMA}.bronze_towers'))
(bronze_calls.write.mode('overwrite').saveAsTable(f'{SCHEMA}.bronze_calls'))

n_towers = spark.table(f'{SCHEMA}.bronze_towers').count()
n_calls = spark.table(f'{SCHEMA}.bronze_calls').count()
print(f"bronze_towers = {n_towers} (should be 120)")
print(f"bronze_calls  = {n_calls} (should be 376200)")
assert n_towers == 120
assert n_calls == 376200

# COMMAND ----------

# MAGIC %md ## Step 3 - silver
# MAGIC This is where most of the actual work is. Logging every reject with a reason
# MAGIC instead of just dropping rows, so I can show what got thrown out and why.

# COMMAND ----------

# duration_seconds / start_ts -> try_cast, not cast (ANSI mode throws on bad values otherwise)
# 1200 rows have 'NA' in duration -> becomes NULL -> these get rejected below
casted = spark.table(f'{SCHEMA}.bronze_calls').selectExpr(
    '*',
    'try_cast(duration_seconds as int) as duration_int',
    'try_cast(start_ts as timestamp) as start_ts_ts'
)

reject_duration_na = casted.filter('duration_int IS NULL').withColumn(
    'reason', F.lit('duration_not_numeric'))
n_dur_na = reject_duration_na.count()
print(f"rejected, duration not numeric = {n_dur_na} (should be 1200)")
assert n_dur_na == 1200

# dedupe on call_id - keep the earliest by ingest time. some rows are exact repeats
# (simulating the collector re-sending the same batch)
w_dedupe = Window.partitionBy('call_id').orderBy('_ingested_at')
deduped = (casted.filter('duration_int IS NOT NULL')
    .withColumn('rk', F.row_number().over(w_dedupe))
    .filter('rk = 1').drop('rk'))

n_before_dedupe = casted.filter('duration_int IS NOT NULL').count()
n_after_dedupe = deduped.count()
print(f"before dedupe = {n_before_dedupe}, after dedupe = {n_after_dedupe}")
assert n_before_dedupe - n_after_dedupe == 1800

# orphan towers (tower_id T999 doesn't exist in the master list) and bad durations (-1)
reject_orphan = deduped.filter("tower_id = 'T999'").withColumn('reason', F.lit('unknown_tower'))
n_orphan = reject_orphan.count()
print(f"rejected, unknown tower = {n_orphan} (should be 600)")
assert n_orphan == 600

reject_range = deduped.filter("tower_id != 'T999' AND (duration_int < 0 OR duration_int > 7200)")
reject_range = reject_range.withColumn('reason', F.lit('duration_out_of_range'))
n_range = reject_range.count()
print(f"rejected, duration out of range = {n_range} (should be 450)")
assert n_range == 450

clean = deduped.filter(
    "tower_id != 'T999' AND duration_int BETWEEN 0 AND 7200"
)
n_clean = clean.count()
print(f"clean rows left = {n_clean} (should be 372150)")
assert n_clean == 372150

# end_cause has some rows padded with spaces + lowercased - trim/upper before comparing
# defining is_dropped exactly once here so it's consistent everywhere downstream
silver_calls = clean.selectExpr(
    'call_id', 'subscriber_id', 'tower_id',
    'start_ts_ts as start_ts',
    'duration_int as duration_seconds',
    'upper(trim(end_cause)) as end_cause',
    'direction', 'technology'
).withColumn('is_dropped', F.col('end_cause') == 'DROPPED') \
 .withColumn('end_ts', F.expr('start_ts + make_interval(0,0,0,0,0,0,duration_seconds)'))

silver_calls.write.mode('overwrite').saveAsTable(f'{SCHEMA}.silver_calls')

# combine all the reject reasons into one table so it's easy to audit later
silver_rejects = (
    reject_duration_na.select('call_id', 'reason')
    .unionByName(reject_orphan.select('call_id', 'reason'))
    .unionByName(reject_range.select('call_id', 'reason'))
)
silver_rejects.write.mode('overwrite').saveAsTable(f'{SCHEMA}.silver_rejects')

# towers table just needs the numeric/date columns cast properly, nothing else to clean
silver_towers = spark.table(f'{SCHEMA}.bronze_towers').selectExpr(
    'tower_id', 'site_name', 'city', 'district',
    'try_cast(capacity_channels as int) as capacity_channels',
    'try_cast(commissioned_on as date) as commissioned_on'
)
silver_towers.write.mode('overwrite').saveAsTable(f'{SCHEMA}.silver_towers')

print("silver done")

# COMMAND ----------

# MAGIC %md ## Step 4 - gold
# MAGIC One row per tower/date/hour. The tricky part is peak_concurrent - had to build it
# MAGIC as a +1/-1 running sum over call start/end events rather than anything simpler.

# COMMAND ----------

calls = spark.table(f'{SCHEMA}.silver_calls')
towers = spark.table(f'{SCHEMA}.silver_towers')

base = calls.withColumn('call_date', F.to_date('start_ts')) \
            .withColumn('hour_of_day', F.hour('start_ts'))

agg = base.groupBy('tower_id', 'call_date', 'hour_of_day').agg(
    F.count('*').alias('calls'),
    F.sum(F.col('is_dropped').cast('int')).alias('dropped_calls'),
    F.avg('duration_seconds').alias('avg_duration_seconds')
)

# peak_concurrent: treat every call start as +1 and every call end as -1, run a cumulative
# sum ordered by time, and take the max of that per tower/date/hour bucket
starts = base.select('tower_id', 'call_date', 'hour_of_day',
                      F.col('start_ts').alias('ts'), F.lit(1).alias('delta'))
ends = base.select('tower_id', 'call_date', 'hour_of_day',
                    F.col('end_ts').alias('ts'), F.lit(-1).alias('delta'))
events = starts.unionByName(ends)

w_run = Window.partitionBy('tower_id', 'call_date', 'hour_of_day') \
              .orderBy('ts', F.desc('delta')) \
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)
running = events.withColumn('running', F.sum('delta').over(w_run))
peak = running.groupBy('tower_id', 'call_date', 'hour_of_day').agg(
    F.max('running').alias('peak_concurrent'))

gold = (agg.join(peak, ['tower_id', 'call_date', 'hour_of_day'], 'left')
    .join(towers, 'tower_id', 'left')
    .withColumn('drop_rate', F.col('dropped_calls') / F.col('calls'))
    .withColumn('utilisation', F.col('peak_concurrent') / F.col('capacity_channels'))
    .select('tower_id', 'site_name', 'city', 'district', 'capacity_channels',
            'call_date', 'hour_of_day', 'calls', 'dropped_calls', 'drop_rate',
            'peak_concurrent', 'utilisation', 'avg_duration_seconds'))

gold.write.mode('overwrite').saveAsTable(f'{SCHEMA}.gold_tower_hour')

n_gold = gold.count()
sum_calls = gold.agg(F.sum('calls')).first()[0]
sum_drops = gold.agg(F.sum('dropped_calls')).first()[0]
print(f"gold rows = {n_gold} (should be 43142)")
print(f"total calls = {sum_calls} (should be 372150)")
print(f"total dropped = {sum_drops} (should be 6792)")
print(f"overall drop rate = {round(100*sum_drops/sum_calls, 2)}%  -- ratio of sums, not avg of hourly %s")

# COMMAND ----------

# MAGIC %md ## Step 5 - export gold to a single csv

# COMMAND ----------

(gold.coalesce(1).write.mode('overwrite').option('header', True)
 .csv(f'{VOL}/export/gold_tower_hour'))

print(f"exported to {VOL}/export/gold_tower_hour")

# COMMAND ----------

# MAGIC %md ## Step 6 - scheduling + reload check
# MAGIC Wrapped this notebook in a Databricks Job on a daily interval (Serverless), ran it once
# MAGIC manually to confirm it completes unattended end to end.
# MAGIC
# MAGIC For the "run twice = zero rows the second time" check - that part happens on the
# MAGIC Snowflake side (COPY INTO), see the .sql file. First run loaded 43142 rows, second
# MAGIC run against the same staged file reported 0 files processed.
