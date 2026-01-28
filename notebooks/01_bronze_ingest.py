# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: Ingest FMUCD from Kaggle
# MAGIC
# MAGIC Download `Facility Management Unified Classification Database (FMUCD).csv` from Kaggle and ingest into Bronze Delta table.
# MAGIC - Install Kaggle API
# MAGIC - Download dataset to Databricks Volume
# MAGIC - Read CSV with schema infer or explicit schema
# MAGIC - Add `_ingest_ts`, `_source_file`
# MAGIC - Write to `{catalog}.{bronze_schema}.bronze_fmucd_raw`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Kaggle API

# COMMAND ----------

# MAGIC %pip install kaggle

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configure Kaggle Credentials
# MAGIC
# MAGIC **Update with your Kaggle credentials:**
# MAGIC - Get your Kaggle API credentials from: https://www.kaggle.com/settings
# MAGIC - Download `kaggle.json` and extract username and key

# COMMAND ----------

import os

# Update with your Kaggle credentials
os.environ["KAGGLE_USERNAME"] = ""  # Your Kaggle username
os.environ["KAGGLE_KEY"] = ""       # Your Kaggle API key

print("Kaggle credentials configured!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Schema and Volume

# COMMAND ----------

# Config: override with your catalog/schema (or load from conf/config.yaml)
CATALOG = "fmucd_capstone"
BRONZE_SCHEMA = "bronze"
TABLE_RAW = "bronze_fmucd_raw"
VOLUME_SCHEMA = "fmucd_data"  # Schema for the volume
VOLUME_NAME = "fmucd_data"    # Volume name

# Update with your Kaggle dataset path (e.g., "username/dataset-name")
KAGGLE_DATASET = ""  # e.g., "your-username/facility-management-unified-classification-database-fmucd"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{VOLUME_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{VOLUME_SCHEMA}.{VOLUME_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Download Dataset from Kaggle

# COMMAND ----------

# Build Volume path (for reference)
VOLUME_PATH = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME_NAME}"
print(f"Volume path: {VOLUME_PATH}")
print(f"Update the %sh commands below with your KAGGLE_DATASET: {KAGGLE_DATASET}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Download Dataset from Kaggle
# MAGIC
# MAGIC **Update the command below with your actual Kaggle dataset path**  
# MAGIC Replace `YOUR_KAGGLE_DATASET_PATH` with the value from `KAGGLE_DATASET` variable above.

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/fmucd_capstone/fmucd_data/fmucd_data
# MAGIC kaggle datasets download -d YOUR_KAGGLE_DATASET_PATH
# MAGIC # Replace YOUR_KAGGLE_DATASET_PATH above with your actual dataset (e.g., "username/dataset-name")
# MAGIC # Or use the KAGGLE_DATASET variable value from the Python cell above

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Unzip Dataset

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/fmucd_capstone/fmucd_data/fmucd_data
# MAGIC unzip -o *.zip
# MAGIC ls -lh

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Clean Up Zip File

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/fmucd_capstone/fmucd_data/fmucd_data
# MAGIC rm -f *.zip
# MAGIC ls -lh

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Read CSV and Ingest to Bronze

# COMMAND ----------

from pyspark.sql import functions as F

# Path to CSV in Volume
FMUCD_CSV_PATH = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME_NAME}/Facility Management Unified Classification Database (FMUCD).csv"

# If the filename is different after unzip, update the path above
# You can check the actual filename with:
# %sh ls -lh /Volumes/fmucd_capstone/fmucd_data/fmucd_data/

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
# MAGIC ## 8. Verify Ingestion

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
# MAGIC **Note**: 
# MAGIC - Update `KAGGLE_DATASET` with your actual Kaggle dataset path (e.g., "username/dataset-name")
# MAGIC - Update `KAGGLE_USERNAME` and `KAGGLE_KEY` with your credentials
# MAGIC - If the CSV filename differs after unzip, update `FMUCD_CSV_PATH` accordingly
