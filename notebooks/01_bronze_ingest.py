# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: Ingest FMUCD
# MAGIC
# MAGIC Ingest `Facility Management Unified Classification Database (FMUCD).csv` from Databricks Volume into Bronze Delta table.
# MAGIC - Read CSV from Volume with schema infer or explicit schema
# MAGIC - Add `_ingest_ts`, `_source_file`
# MAGIC - Write to `{catalog}.{bronze_schema}.bronze_fmucd_raw`

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Config: override with your catalog/schema/path (or load from conf/config.yaml)
CATALOG = "fmucd_capstone"
BRONZE_SCHEMA = "bronze"
TABLE_RAW = "bronze_fmucd_raw"

# Path to CSV in Volume (already uploaded)
FMUCD_CSV_PATH = "/Volumes/workspace/sor/fmucd/Facility Management Unified Classification Database (FMUCD).csv"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read CSV and Ingest to Bronze

# COMMAND ----------

df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(FMUCD_CSV_PATH)
)
df = df.withColumn("_ingest_ts", F.current_timestamp()).withColumn(
    "_source_file", F.lit("FMUCD.csv")
)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable(
    f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE_RAW}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check row count
# MAGIC SELECT COUNT(*) as row_count
# MAGIC FROM fmucd_capstone.bronze.bronze_fmucd_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample records
# MAGIC SELECT * 
# MAGIC FROM fmucd_capstone.bronze.bronze_fmucd_raw
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC **Note**: The CSV file is already uploaded to `/Volumes/workspace/sor/fmucd/`. If the path changes, update `FMUCD_CSV_PATH` variable above.
