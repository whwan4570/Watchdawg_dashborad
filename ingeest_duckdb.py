# ingest_duckdb.py
import duckdb, pandas as pd

DB = "crime.duckdb"
CSV = "SPD_Crime_Data__2008-Present_20251120.csv"

con = duckdb.connect(DB)

# 1) Load raw data and select minimum columns
con.execute(f"""
CREATE OR REPLACE TABLE raw AS
SELECT * FROM read_csv_auto('{CSV}', header=True);
""")

# Determine optional columns for downstream transforms
raw_columns = {row[1] for row in con.execute("PRAGMA table_info('raw')").fetchall()}
has_mcpp = "MCPP Neighborhood" in raw_columns
has_nibrs_code = "NIBRS Offense Code" in raw_columns
has_offense_sub = "Offense Sub Category" in raw_columns

mcpp_expr = '"MCPP Neighborhood"' if has_mcpp else "NULL"
nibrs_expr = '"NIBRS Offense Code"' if has_nibrs_code else "NULL"
offense_sub_expr = '"Offense Sub Category"' if has_offense_sub else "NULL"

filters = [
    "dt IS NOT NULL",
    "year >= 2008",
    "latitude BETWEEN 47.0 AND 48.1",
    "longitude BETWEEN -123.5 AND -121.0",
    "sector <> '99'",
    "precinct <> 'OOJ'"
]
if has_nibrs_code:
    filters.append("nibrs_offense_code <> '999'")
if has_offense_sub:
    filters.append("offense_sub_category <> '999'")
if has_mcpp:
    filters.append("mcpp <> 'UNKNOWN'")

filters_sql = "\n  AND ".join(filters)

# 2) Create normalized table (date parsing/extraction + 2008 filter + column normalization)
con.execute(f"""
CREATE OR REPLACE TABLE crimes AS
WITH t AS (
  SELECT
    /* Combine datetime candidates into one (cast to VARCHAR) */
    CAST(COALESCE("Offense Date","Report DateTime") AS VARCHAR) AS dt_raw,
    /* Normalize offense/location/area */
    UPPER(COALESCE("Offense Sub Category","Offense Category",
                   "NIBRS Offense Code Description",
                   "NIBRS Crime Against Category",'UNKNOWN')) AS offense,
    COALESCE("Block Address",'') AS location,
    COALESCE("Neighborhood","Precinct","Sector",'') AS area,
    COALESCE("Precinct",'') AS precinct,
    COALESCE("Sector",'') AS sector,
    COALESCE({mcpp_expr},'') AS mcpp,
    COALESCE({nibrs_expr},'') AS nibrs_offense_code,
    COALESCE({offense_sub_expr},'') AS offense_sub_category,
    CASE 
      WHEN "Latitude" = 'REDACTED' OR "Latitude" = '' OR "Latitude" IS NULL 
      THEN NULL 
      ELSE CAST("Latitude" AS DOUBLE) 
    END AS latitude,
    CASE 
      WHEN "Longitude" = 'REDACTED' OR "Longitude" = '' OR "Longitude" IS NULL 
      THEN NULL 
      ELSE CAST("Longitude" AS DOUBLE) 
    END AS longitude
  FROM raw
),
p AS (
  SELECT
    try_strptime(dt_raw, '%Y-%m-%d %H:%M:%S') AS dt1,
    try_strptime(dt_raw, '%Y-%m-%dT%H:%M:%S') AS dt2,
    try_strptime(dt_raw, '%m/%d/%Y %H:%M:%S') AS dt3,
    try_strptime(dt_raw, '%m/%d/%Y %I:%M:%S %p') AS dt4,
    try_strptime(dt_raw, '%Y-%m-%d') AS dt5,
    try_strptime(dt_raw, '%Y %b %d %I:%M:%S %p') AS dt6,
    offense,
    location,
    area,
    precinct,
    sector,
    mcpp,
    nibrs_offense_code,
    offense_sub_category,
    latitude,
    longitude
  FROM t
),
u AS (
  SELECT
    COALESCE(dt1,dt2,dt3,dt4,dt5,dt6) AS dt,
    offense,
    location,
    area,
    precinct,
    sector,
    mcpp,
    nibrs_offense_code,
    offense_sub_category,
    latitude,
    longitude
  FROM p
)
SELECT
  CAST(dt AS DATE) AS date,
  strftime(dt, '%H:%M:%S') AS time,
  CAST(strftime(dt, '%H') AS INTEGER) AS hour,
  CAST(strftime(dt, '%Y') AS INTEGER) AS year,
  offense,
  location,
  area,
  precinct,
  sector,
  mcpp,
  nibrs_offense_code,
  offense_sub_category,
  CASE
    WHEN sector = '99'
      OR precinct = 'OOJ'
      OR mcpp = 'UNKNOWN'
    THEN TRUE
    ELSE FALSE
  END AS is_out_of_area,
  latitude,
  longitude
FROM u
WHERE {filters_sql};
""")

# 3) Create daily aggregation view
con.execute("""
CREATE OR REPLACE VIEW kpi_by_date AS
SELECT
  date,
  COUNT(*) AS total,
  SUM( CASE WHEN offense LIKE '%ASSAULT%' OR offense LIKE '%ROBBERY%' OR
                 offense LIKE '%HOMICIDE%' OR offense LIKE '%RAPE%' OR
                 offense LIKE '%SEX OFFENSE%' THEN 1 ELSE 0 END ) AS violent,
  SUM( CASE WHEN offense LIKE '%THEFT%' OR offense LIKE '%BURGLARY%' OR
                 offense LIKE '%CAR PROWL%' OR offense LIKE '%SHOPLIFTING%' OR
                 offense LIKE '%MOTOR VEHICLE THEFT%' OR offense LIKE '%ARSON%' OR
                 offense LIKE '%VANDALISM%' THEN 1 ELSE 0 END ) AS property,
  COUNT(*) 
    - ( /* violent + property */ 
        SUM( CASE WHEN offense LIKE '%ASSAULT%' OR offense LIKE '%ROBBERY%' OR
                       offense LIKE '%HOMICIDE%' OR offense LIKE '%RAPE%' OR
                       offense LIKE '%SEX OFFENSE%' THEN 1 ELSE 0 END )
      + SUM( CASE WHEN offense LIKE '%THEFT%' OR offense LIKE '%BURGLARY%' OR
                       offense LIKE '%CAR PROWL%' OR offense LIKE '%SHOPLIFTING%' OR
                       offense LIKE '%MOTOR VEHICLE THEFT%' OR offense LIKE '%ARSON%' OR
                       offense LIKE '%VANDALISM%' THEN 1 ELSE 0 END )
      ) AS other
FROM crimes
GROUP BY date;
""")

# 4) Check date range
print(con.execute("SELECT MIN(date), MAX(date), COUNT(*) FROM crimes").fetchall())
con.close()

