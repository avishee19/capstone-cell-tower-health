-- Capstone 31 - Cell Tower Health & Dropped Calls
-- Snowflake side: stage the exported gold csv, load it, answer the 3 questions.
-- No prefix needed on db/schema names here since this is my own Snowflake account.

CREATE DATABASE IF NOT EXISTS CAPSTONE;
CREATE SCHEMA IF NOT EXISTS CAPSTONE.TOWER31;
USE SCHEMA CAPSTONE.TOWER31;

-- ---------------------------------------------------------
-- stage setup
-- ---------------------------------------------------------
CREATE OR REPLACE FILE FORMAT CSV_HEADER
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL');

CREATE OR REPLACE STAGE GOLD_STAGE
  FILE_FORMAT = CSV_HEADER;

-- uploaded the exported csv from my databricks volume through the Snowsight upload UI
-- (stage upload dialog had a rendering bug where the confirm button was cut off on my
-- screen - had to zoom the browser out to actually see it, worth noting in case this
-- happens again)

-- ---------------------------------------------------------
-- table + load
-- ---------------------------------------------------------
CREATE OR REPLACE TABLE GOLD_TOWER_HOUR (
    tower_id             STRING,
    site_name            STRING,
    city                 STRING,
    district             STRING,
    capacity_channels    INT,
    call_date            DATE,
    hour_of_day          INT,
    calls                INT,
    dropped_calls        INT,
    drop_rate            FLOAT,
    peak_concurrent       INT,
    utilisation          FLOAT,
    avg_duration_seconds FLOAT
);

COPY INTO GOLD_TOWER_HOUR
  FROM @GOLD_STAGE
  FILE_FORMAT = CSV_HEADER
  ON_ERROR = 'ABORT_STATEMENT';
-- first run: loaded 43142 rows, matches gold row count from databricks

-- ran this exact same statement again afterward to check idempotency -
-- second run reported "Copy executed with 0 files processed", which is what
-- proves the load metadata is tracking the file properly and not double-loading it

-- quick check against the numbers from the databricks notebook
SELECT COUNT(*) AS gold_rows,                                  -- 43142
       SUM(calls) AS total_calls,                               -- 372150
       SUM(dropped_calls) AS total_dropped,                     -- 6792
       ROUND(100.0*SUM(dropped_calls)/NULLIF(SUM(calls),0), 2) AS circle_drop_pct
FROM GOLD_TOWER_HOUR;
-- got 1.83% here vs 1.82% in the brief - checked the math, it's just a rounding
-- boundary thing (true value is 1.8251%), the actual sums match exactly so
-- I'm treating this as correct, not a bug

-- ============================================================
-- Q1: drop rate per tower per hour, and each tower's worst hour
-- ============================================================
-- important part here: SUM(dropped)/SUM(calls), not AVG of per-hour percentages.
-- averaging the ratios gives a totally different (wrong) answer - found this out
-- the hard way while double checking my numbers against the brief
WITH th AS (
  SELECT tower_id, hour_of_day,
         SUM(calls) AS calls, SUM(dropped_calls) AS drops
  FROM GOLD_TOWER_HOUR
  GROUP BY tower_id, hour_of_day)
SELECT tower_id, hour_of_day, calls, drops,
       ROUND(100.0 * drops / NULLIF(calls, 0), 2) AS worst_hour_pct,
       ROUND(100.0 * SUM(drops) OVER (PARTITION BY tower_id)
             / NULLIF(SUM(calls) OVER (PARTITION BY tower_id), 0), 2) AS tower_pct
FROM th
QUALIFY ROW_NUMBER() OVER (PARTITION BY tower_id
         ORDER BY drops / NULLIF(calls, 0) DESC, hour_of_day) = 1
ORDER BY worst_hour_pct DESC;

-- ============================================================
-- Q2: towers with >5% drop rate on more than 10 of the 15 days
-- ============================================================
WITH daily AS (
  SELECT tower_id, call_date,
         SUM(calls) AS calls, SUM(dropped_calls) AS drops
  FROM GOLD_TOWER_HOUR
  GROUP BY tower_id, call_date
),
flagged AS (
  SELECT tower_id, call_date,
         100.0 * drops / NULLIF(calls, 0) AS drop_pct
  FROM daily
)
SELECT tower_id, COUNT(*) AS days_over_5pct
FROM flagged
WHERE drop_pct > 5
GROUP BY tower_id
HAVING COUNT(*) > 10
ORDER BY days_over_5pct DESC;

-- ============================================================
-- Q3: does drop rate rise with load, or is it flat?
-- ============================================================
-- bucketing by utilisation (peak_concurrent / capacity), NOT raw peak_concurrent -
-- tried raw peak_concurrent first out of curiosity and the relationship actually
-- inverts, since the weak towers only have 2 channels so they "peak" easily even
-- at low absolute call volume. utilisation is the fair comparison across towers.
SELECT ROUND(utilisation, 1) AS util_bucket,
       SUM(calls) AS calls,
       SUM(dropped_calls) AS drops,
       ROUND(100.0 * SUM(dropped_calls) / NULLIF(SUM(calls), 0), 2) AS drop_pct
FROM GOLD_TOWER_HOUR
GROUP BY ROUND(utilisation, 1)
ORDER BY util_bucket;
-- got roughly 0.27 / 0.56 / 1.40 / 12.57 percent across buckets 0.1/0.2/0.4/0.5 -
-- matches the brief, and it's a big enough jump that it's clearly not flat
