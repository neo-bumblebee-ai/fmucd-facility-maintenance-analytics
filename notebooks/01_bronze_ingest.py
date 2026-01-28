# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: Ingest FMUCD
# MAGIC
# MAGIC Ingest `Facility Management Unified Classification Database (FMUCD).csv` from Databricks Volume into Bronze Delta table.
# MAGIC - Read CSV from Volume with robust parsing options
# MAGIC - Handle quoted fields, multi-line values, and special characters
# MAGIC - Add `_ingest_ts`, `_source_file`
# MAGIC - Write to `{catalog}.{bronze_schema}.bronze_fmucd_raw`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

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
# MAGIC ## Diagnostic: Inspect CSV Format (Optional)
# MAGIC
# MAGIC Run this section if you encounter parsing issues to understand the CSV structure.

# COMMAND ----------

# MAGIC %sh
# MAGIC # Check first few lines of CSV to understand format
# MAGIC head -n 3 "/Volumes/workspace/sor/fmucd/Facility Management Unified Classification Database (FMUCD).csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read CSV with Robust Parsing Options
# MAGIC
# MAGIC **Note**: If schema inference causes issues, you can uncomment the explicit schema section below and comment out the inferSchema approach.

# COMMAND ----------

# Optional: Explicit schema definition (uncomment if inferSchema causes issues)
# Based on FMUCD structure from earlier inspection
# explicit_schema = StructType([
#     StructField("UniversityID", IntegerType(), True),
#     StructField("Country", StringType(), True),
#     StructField("State/Province", StringType(), True),
#     StructField("BuildingID", StringType(), True),
#     StructField("BuildingName", StringType(), True),
#     StructField("Size", DoubleType(), True),
#     StructField("Type", StringType(), True),
#     StructField("BuiltYear", DoubleType(), True),
#     StructField("FCI (facility condition index)", DoubleType(), True),
#     StructField("CRV (current replacement value)", DoubleType(), True),
#     StructField("DMC (deferred maintenance cost)", DoubleType(), True),
#     StructField("SystemCode", StringType(), True),
#     StructField("SystemDescription", StringType(), True),
#     StructField("SubsystemCode", StringType(), True),
#     StructField("SubsystemDescription", StringType(), True),
#     StructField("DescriptiveCode", StringType(), True),
#     StructField("ComponentDescription", StringType(), True),
#     StructField("WOID", StringType(), True),
#     StructField("WODescription", StringType(), True),
#     StructField("WOPriority", IntegerType(), True),
#     StructField("WOStartDate", StringType(), True),  # Read as string, convert later
#     StructField("WOEndDate", StringType(), True),
#     StructField("WODuration", DoubleType(), True),
#     StructField("PPM/UPM", StringType(), True),
#     StructField("LaborCost", DoubleType(), True),
#     StructField("MaterialCost", DoubleType(), True),
#     StructField("OtherCost", DoubleType(), True),
#     StructField("TotalCost", DoubleType(), True),
#     StructField("LaborHours", DoubleType(), True),
#     StructField("MinTemp.(°C)", DoubleType(), True),
#     StructField("MaxTemp.(°C)", DoubleType(), True),
#     StructField("Atmospheric pressure(hPa)", DoubleType(), True),
#     StructField("Humidity(%)", DoubleType(), True),
#     StructField("WindSpeed(m/s)", DoubleType(), True),
#     StructField("WindDegree", DoubleType(), True),
#     StructField("Precipitation(mm)", DoubleType(), True),
#     StructField("Snow(mm)", DoubleType(), True),
#     StructField("Cloudness(%)", DoubleType(), True),
# ])

# COMMAND ----------

# Read CSV with enhanced options for proper parsing
# Option 1: Use explicit schema (uncomment if inferSchema causes issues)
# df = (
#     spark.read
#     .option("header", "true")
#     .schema(explicit_schema)  # Use explicit schema defined above
#     .option("delimiter", ",")
#     .option("quote", '"')
#     .option("escape", '"')
#     .option("multiLine", "true")
#     .option("ignoreLeadingWhiteSpace", "true")
#     .option("ignoreTrailingWhiteSpace", "true")
#     .option("nullValue", "")
#     .option("emptyValue", "")
#     .option("encoding", "UTF-8")
#     .option("mode", "PERMISSIVE")
#     .option("columnNameOfCorruptRecord", "_corrupt_record")
#     .csv(FMUCD_CSV_PATH)
# )

# Option 2: Use schema inference (default - try this first)
# Try with comma delimiter first (most common)
try:
    df = (
        spark.read
        .option("header", "true")                    # First row is header
        .option("inferSchema", "true")               # Infer data types
        .option("delimiter", ",")                    # Comma delimiter
        .option("quote", '"')                        # Double quote for quoted fields
        .option("escape", '"')                       # Escape quotes within quoted fields
        .option("multiLine", "true")                 # Handle multi-line values
        .option("ignoreLeadingWhiteSpace", "true")   # Trim leading whitespace
        .option("ignoreTrailingWhiteSpace", "true")  # Trim trailing whitespace
        .option("nullValue", "")                     # Treat empty strings as null
        .option("emptyValue", "")                    # Handle empty values
        .option("encoding", "UTF-8")                 # UTF-8 encoding
        .option("mode", "PERMISSIVE")                # Handle malformed records gracefully
        .option("columnNameOfCorruptRecord", "_corrupt_record")  # Capture bad records
        .csv(FMUCD_CSV_PATH)
    )
    print("✅ CSV read successfully with comma delimiter")
except Exception as e:
    print(f"⚠️ Failed with comma delimiter: {e}")
    print("Trying with semicolon delimiter...")
    # Fallback: try semicolon delimiter (common in European CSVs)
    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ";")
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", "true")
            .option("ignoreLeadingWhiteSpace", "true")
            .option("ignoreTrailingWhiteSpace", "true")
            .option("nullValue", "")
            .option("emptyValue", "")
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(FMUCD_CSV_PATH)
        )
        print("✅ CSV read successfully with semicolon delimiter")
    except Exception as e2:
        print(f"❌ Failed with semicolon delimiter: {e2}")
        print("Please check the CSV format and delimiter. You may need to use explicit schema.")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for Parsing Issues

# COMMAND ----------

# Check for corrupt records
if "_corrupt_record" in df.columns:
    corrupt_count = df.filter(F.col("_corrupt_record").isNotNull()).count()
    if corrupt_count > 0:
        print(f"⚠️ Warning: {corrupt_count} corrupt records detected!")
        print("Sample corrupt records:")
        df.filter(F.col("_corrupt_record").isNotNull()).select("_corrupt_record").show(5, truncate=False)
    else:
        print("✅ No corrupt records detected")
        # Drop the _corrupt_record column if not needed
        df = df.drop("_corrupt_record")

# COMMAND ----------

# Display schema and sample
print("Schema:")
df.printSchema()

print("\nRow count:")
print(df.count())

print("\nSample records:")
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Metadata Columns and Ingest to Bronze

# COMMAND ----------

# Add ingestion metadata
df = df.withColumn("_ingest_ts", F.current_timestamp()).withColumn(
    "_source_file", F.lit("FMUCD.csv")
)

# COMMAND ----------

# Write to Bronze Delta table
df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable(
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
# MAGIC -- Check column count
# MAGIC SELECT COUNT(*) as column_count
# MAGIC FROM (
# MAGIC   SELECT * FROM fmucd_capstone.bronze.bronze_fmucd_raw LIMIT 1
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample records
# MAGIC SELECT * 
# MAGIC FROM fmucd_capstone.bronze.bronze_fmucd_raw
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for nulls in key columns
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_rows,
# MAGIC   SUM(CASE WHEN WOID IS NULL THEN 1 ELSE 0 END) as null_woid,
# MAGIC   SUM(CASE WHEN SystemCode IS NULL THEN 1 ELSE 0 END) as null_systemcode,
# MAGIC   SUM(CASE WHEN BuildingID IS NULL THEN 1 ELSE 0 END) as null_buildingid
# MAGIC FROM fmucd_capstone.bronze.bronze_fmucd_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC **Note**: 
# MAGIC - The CSV file is already uploaded to `/Volumes/workspace/sor/fmucd/`
# MAGIC - Enhanced CSV parsing options handle quoted fields, multi-line values, and special characters
# MAGIC - If issues persist, check the `_corrupt_record` column for problematic rows
