# Databricks notebook source
# MAGIC %md
# MAGIC # 02b — Data Quality Checks
# MAGIC
# MAGIC Run after Silver. Checks: nulls (key columns), duplicate WOID, value ranges (WOPriority, work_type).
# MAGIC Log results or raise on failure.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "fmucd_capstone"
SILVER_SCHEMA = "silver"
TABLE_WO = "silver_work_orders"

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_WO}")

# Null counts on key columns
key_cols = ["WOID", "SystemCode", "work_type", "TotalCost"]
for c in key_cols:
    null_cnt = df.filter(F.col(c).isNull()).count()
    print(f"  {c}: {null_cnt} nulls")

# Duplicate WOID (should be 0 after Silver dedup)
dup = df.groupBy("WOID").agg(F.count("*").alias("n")).filter(F.col("n") > 1)
dup_cnt = dup.count()
print(f"  Duplicate WOID rows: {dup_cnt}")

# Work type distribution
print("  work_type counts:")
df.groupBy("work_type").count().orderBy(F.desc("count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Optional**: Assert `dup_cnt == 0` and null counts below thresholds; fail Job step if not.
