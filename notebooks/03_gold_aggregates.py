# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: Aggregates & KPIs
# MAGIC
# MAGIC - Read from Silver.
# MAGIC - Build Gold tables: by building, by system, PPM vs UPM summary, KPIs.
# MAGIC - Use Delta OPTIMIZE / ZORDER for query patterns.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "fmucd_capstone"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
TABLE_WO = "silver_work_orders"
TABLE_BY_BLDG = "gold_wo_by_building"
TABLE_BY_SYS = "gold_wo_by_system"
TABLE_PPM_UPM = "gold_ppm_upm_summary"
TABLE_KPIS = "gold_kpis"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

wo = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_WO}")

# Cast numeric columns for aggregations
wo = wo.withColumn("TotalCost", F.coalesce(F.col("TotalCost").cast("double"), F.lit(0.0)))
wo = wo.withColumn("LaborHours", F.coalesce(F.col("LaborHours").cast("double"), F.lit(0.0)))

# Gold: by building
by_bldg = (
    wo.groupBy("BuildingID", "BuildingName", "SystemCode", "work_type")
    .agg(
        F.count("*").alias("wo_count"),
        F.sum("TotalCost").alias("total_cost"),
        F.sum("LaborHours").alias("labor_hours"),
    )
)
by_bldg.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{GOLD_SCHEMA}.{TABLE_BY_BLDG}"
)

# Gold: by system
by_sys = (
    wo.groupBy("SystemCode", "SystemDescription", "work_type")
    .agg(
        F.count("*").alias("wo_count"),
        F.sum("TotalCost").alias("total_cost"),
        F.sum("LaborHours").alias("labor_hours"),
    )
)
by_sys.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{GOLD_SCHEMA}.{TABLE_BY_SYS}"
)

# Gold: PPM vs UPM summary
ppm_upm = (
    wo.groupBy("work_type")
    .agg(
        F.count("*").alias("wo_count"),
        F.sum("TotalCost").alias("total_cost"),
        F.sum("LaborHours").alias("labor_hours"),
    )
)
ppm_upm.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{GOLD_SCHEMA}.{TABLE_PPM_UPM}"
)

# Gold: KPIs (single-row or small table)
kpis = wo.agg(
    F.count("*").alias("total_wo"),
    F.sum("TotalCost").alias("total_cost"),
    F.sum("LaborHours").alias("total_labor_hrs"),
    F.countDistinct("WOID").alias("distinct_wo"),
)
kpis.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{GOLD_SCHEMA}.{TABLE_KPIS}"
)

# COMMAND ----------

# OPTIMIZE / ZORDER (run periodically)
spark.sql(f"OPTIMIZE {CATALOG}.{GOLD_SCHEMA}.{TABLE_BY_BLDG} ZORDER BY (BuildingID, SystemCode)")
spark.sql(f"OPTIMIZE {CATALOG}.{GOLD_SCHEMA}.{TABLE_BY_SYS} ZORDER BY (SystemCode, work_type)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Next**: SQL dashboard on Gold tables; optional ML feature table from Silver/Gold.
