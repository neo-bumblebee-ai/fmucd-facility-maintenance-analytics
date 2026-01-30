# Databricks notebook source
# 01 — Bronze: Ingest FMUCD (Single CSV in Volumes)

from pyspark.sql import functions as F

# -------------------------
# CONFIG
# -------------------------
CATALOG = "fmucd_capstone"
BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = "bronze_fmucd_raw"

SOURCE_PATH = "/Volumes/workspace/sor/fmucd/Facility Management Unified Classification Database (FMUCD).csv"

FULL_TABLE_NAME = f"{CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"

# -------------------------
# CREATE NAMESPACES
# -------------------------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# -------------------------
# READ (RAW)
# -------------------------
df_raw = (
    spark.read
    .option("header", "true")                   # CSV header row
    .option("inferSchema", "false")             # Bronze = keep as string, preserve source
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("multiLine", "true")                # allow multi-line text fields
    .option("ignoreLeadingWhiteSpace", "true")
    .option("ignoreTrailingWhiteSpace", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .csv(SOURCE_PATH)
)

# -------------------------
# BASIC PARSING CHECK (optional but useful)
# -------------------------
if "_corrupt_record" in df_raw.columns:
    corrupt_cnt = df_raw.filter(F.col("_corrupt_record").isNotNull()).count()
    if corrupt_cnt > 0:
        print(f"⚠️ Corrupt records detected: {corrupt_cnt}")
        display(df_raw.filter(F.col("_corrupt_record").isNotNull()).select("_corrupt_record").limit(10))
    else:
        # If none exist, drop the column to keep bronze clean
        df_raw = df_raw.drop("_corrupt_record")

# -------------------------
# ADD METADATA
# -------------------------
df_bronze = (
    df_raw
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.lit(SOURCE_PATH))
    .withColumn("_batch_id", F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"))
)

# -------------------------
# WRITE TO DELTA (repeatable)
# -------------------------
(
    df_bronze.write
    .format("delta")
    .mode("overwrite")                 # IMPORTANT: no duplicates when re-running
    .option("overwriteSchema", "true")
    .saveAsTable(FULL_TABLE_NAME)
)

print(f"✅ Bronze table created: {FULL_TABLE_NAME}")
print("Rows:", df_bronze.count())
print("Columns:", len(df_bronze.columns))
display(df_bronze.limit(10))

%sql
-- 1) Count
SELECT COUNT(*) AS row_count
FROM fmucd_capstone.bronze.bronze_fmucd_raw;

-- 2) Ensure header didn't load as a data row
SELECT COUNT(*) AS header_rows
FROM fmucd_capstone.bronze.bronze_fmucd_raw
WHERE Country = 'Country';

-- 3) Key nulls
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN WOID IS NULL THEN 1 ELSE 0 END) AS null_woid,
  SUM(CASE WHEN BuildingID IS NULL THEN 1 ELSE 0 END) AS null_buildingid,
  SUM(CASE WHEN SystemCode IS NULL THEN 1 ELSE 0 END) AS null_systemcode
FROM fmucd_capstone.bronze.bronze_fmucd_raw;
