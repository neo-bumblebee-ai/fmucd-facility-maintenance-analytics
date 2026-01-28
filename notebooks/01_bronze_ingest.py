# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: Ingest FMUCD
# MAGIC
# MAGIC Ingest `Facility Management Unified Classification Database (FMUCD).csv` into a Bronze Delta table.
# MAGIC - Read CSV with schema infer or explicit schema.
# MAGIC - Add `_ingest_ts`, `_source_file`.
# MAGIC - Write to `{catalog}.{bronze_schema}.bronze_fmucd_raw`.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# COMMAND ----------

# Config: override with your catalog/schema/path (or load from conf/config.yaml).
CATALOG = "fmucd_capstone"
BRONZE_SCHEMA = "bronze"
TABLE_RAW = "bronze_fmucd_raw"
# Update path for your environment (workspace, DBFS, or cloud storage).
FMUCD_CSV_PATH = "/Workspace/Repos/<your-repo>/fmucd-facility-maintenance-analytics/docs/Facility Management Unified Classification Database (FMUCD).csv"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

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
# MAGIC **Check**: `SELECT COUNT(*) FROM {catalog}.{bronze_schema}.{table_raw}`
