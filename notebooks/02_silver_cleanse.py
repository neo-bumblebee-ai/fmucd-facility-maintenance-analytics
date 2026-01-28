# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: Cleanse & Validate (Fact & Dimension Tables with SCD Type 2)
# MAGIC
# MAGIC - Read from Bronze.
# MAGIC - Create **Fact table**: `fact_work_orders` (with foreign keys to dimensions).
# MAGIC - Create **Dimension tables** with **SCD Type 2**:
# MAGIC   - `dim_building` (BuildingID, BuildingName, Size, Type, BuiltYear, FCI, CRV, DMC, etc.)
# MAGIC   - `dim_system` (SystemCode, SystemDescription)
# MAGIC   - `dim_subsystem` (SubsystemCode, SubsystemDescription, SystemCode)
# MAGIC   - `dim_component` (DescriptiveCode, ComponentDescription, SubsystemCode)
# MAGIC - SCD Type 2: `start_date`, `end_date`, `is_active` flag.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType, DateType

# COMMAND ----------

CATALOG = "fmucd_capstone"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
TABLE_BRONZE = "bronze_fmucd_raw"

# Fact and Dimension table names
FACT_WO = "fact_work_orders"
DIM_BUILDING = "dim_building"
DIM_SYSTEM = "dim_system"
DIM_SUBSYSTEM = "dim_subsystem"
DIM_COMPONENT = "dim_component"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# Read from Bronze
df_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE_BRONZE}")

# Deduplicate: one row per WOID (keep latest _ingest_ts per WOID)
w = Window.partitionBy("WOID").orderBy(F.col("_ingest_ts").desc())
df_bronze = df_bronze.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

# Normalize PPM/UPM → work_type
df_bronze = df_bronze.withColumn(
    "work_type",
    F.when(F.upper(F.coalesce(F.col("PPM/UPM"), F.lit(""))).contains("PPM"), "PPM")
    .when(F.upper(F.coalesce(F.col("PPM/UPM"), F.lit(""))).contains("UPM"), "UPM")
    .otherwise("Unknown"),
)

# Backfill BuildingID nulls
df_bronze = df_bronze.withColumn(
    "BuildingID",
    F.coalesce(F.col("BuildingID"), F.lit("Campus-level")),
)

# Cast date columns
df_bronze = df_bronze.withColumn(
    "WOStartDate",
    F.to_timestamp(F.col("WOStartDate"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "WOEndDate",
    F.to_timestamp(F.col("WOEndDate"), "yyyy-MM-dd HH:mm:ss")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dimension Tables (SCD Type 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. dim_building (SCD Type 2)

# COMMAND ----------

# Extract unique building records
df_buildings = df_bronze.select(
    "BuildingID",
    "BuildingName",
    "Size",
    "Type",
    "BuiltYear",
    "FCI (facility condition index)",
    "CRV (current replacement value)",
    "DMC (deferred maintenance cost)",
    "Country",
    "State/Province",
).distinct().filter(F.col("BuildingID").isNotNull() & (F.col("BuildingID") != ""))

# Add SCD Type 2 columns
current_date = F.current_date()
df_buildings_new = df_buildings.withColumn("start_date", current_date).withColumn(
    "end_date", F.lit(None).cast(DateType())
).withColumn("is_active", F.lit(True)).withColumn(
    "surrogate_key", F.monotonically_increasing_id()
)

# Check if dim_building exists
try:
    df_buildings_existing = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{DIM_BUILDING}")
    
    # Get active records only
    df_buildings_active = df_buildings_existing.filter(F.col("is_active") == True)
    
    # Identify changed records (compare key attributes)
    df_buildings_changed = df_buildings_new.alias("new").join(
        df_buildings_active.alias("existing"),
        F.col("new.BuildingID") == F.col("existing.BuildingID"),
        "left_anti"
    ).union(
        df_buildings_new.alias("new").join(
            df_buildings_active.alias("existing"),
            (F.col("new.BuildingID") == F.col("existing.BuildingID")) &
            (
                (F.col("new.BuildingName") != F.coalesce(F.col("existing.BuildingName"), F.lit(""))) |
                (F.col("new.Size") != F.coalesce(F.col("existing.Size"), F.lit(0))) |
                (F.col("new.Type") != F.coalesce(F.col("existing.Type"), F.lit(""))) |
                (F.col("new.FCI (facility condition index)") != F.coalesce(F.col("existing.FCI (facility condition index)"), F.lit(0.0)))
            ),
            "inner"
        ).select("new.*")
    )
    
    # Close old records (set end_date and is_active=False)
    df_buildings_to_close = df_buildings_active.alias("existing").join(
        df_buildings_changed.alias("new"),
        F.col("existing.BuildingID") == F.col("new.BuildingID"),
        "inner"
    ).select(
        F.col("existing.*")
    ).withColumn("end_date", F.date_sub(current_date, 1)).withColumn(
        "is_active", F.lit(False)
    )
    
    # Combine: close old + add new
    df_buildings_final = df_buildings_existing.filter(F.col("is_active") == False).union(
        df_buildings_to_close
    ).union(
        df_buildings_changed
    )
    
except:
    # First run: create initial dimension
    df_buildings_final = df_buildings_new

# Write dim_building
df_buildings_final.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.{DIM_BUILDING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. dim_system (SCD Type 2)

# COMMAND ----------

df_systems = df_bronze.select(
    "SystemCode",
    "SystemDescription",
).distinct().filter(F.col("SystemCode").isNotNull() & (F.col("SystemCode") != ""))

df_systems_new = df_systems.withColumn("start_date", current_date).withColumn(
    "end_date", F.lit(None).cast(DateType())
).withColumn("is_active", F.lit(True)).withColumn(
    "surrogate_key", F.monotonically_increasing_id()
)

try:
    df_systems_existing = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{DIM_SYSTEM}")
    df_systems_active = df_systems_existing.filter(F.col("is_active") == True)
    
    df_systems_changed = df_systems_new.alias("new").join(
        df_systems_active.alias("existing"),
        F.col("new.SystemCode") == F.col("existing.SystemCode"),
        "left_anti"
    ).union(
        df_systems_new.alias("new").join(
            df_systems_active.alias("existing"),
            (F.col("new.SystemCode") == F.col("existing.SystemCode")) &
            (F.col("new.SystemDescription") != F.coalesce(F.col("existing.SystemDescription"), F.lit(""))),
            "inner"
        ).select("new.*")
    )
    
    df_systems_to_close = df_systems_active.alias("existing").join(
        df_systems_changed.alias("new"),
        F.col("existing.SystemCode") == F.col("new.SystemCode"),
        "inner"
    ).select(F.col("existing.*")).withColumn("end_date", F.date_sub(current_date, 1)).withColumn(
        "is_active", F.lit(False)
    )
    
    df_systems_final = df_systems_existing.filter(F.col("is_active") == False).union(
        df_systems_to_close
    ).union(df_systems_changed)
    
except:
    df_systems_final = df_systems_new

df_systems_final.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.{DIM_SYSTEM}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. dim_subsystem (SCD Type 2)

# COMMAND ----------

df_subsystems = df_bronze.select(
    "SubsystemCode",
    "SubsystemDescription",
    "SystemCode",
).distinct().filter(
    F.col("SubsystemCode").isNotNull() & (F.col("SubsystemCode") != "")
)

df_subsystems_new = df_subsystems.withColumn("start_date", current_date).withColumn(
    "end_date", F.lit(None).cast(DateType())
).withColumn("is_active", F.lit(True)).withColumn(
    "surrogate_key", F.monotonically_increasing_id()
)

try:
    df_subsystems_existing = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{DIM_SUBSYSTEM}")
    df_subsystems_active = df_subsystems_existing.filter(F.col("is_active") == True)
    
    df_subsystems_changed = df_subsystems_new.alias("new").join(
        df_subsystems_active.alias("existing"),
        F.col("new.SubsystemCode") == F.col("existing.SubsystemCode"),
        "left_anti"
    ).union(
        df_subsystems_new.alias("new").join(
            df_subsystems_active.alias("existing"),
            (F.col("new.SubsystemCode") == F.col("existing.SubsystemCode")) &
            (
                (F.col("new.SubsystemDescription") != F.coalesce(F.col("existing.SubsystemDescription"), F.lit(""))) |
                (F.col("new.SystemCode") != F.coalesce(F.col("existing.SystemCode"), F.lit("")))
            ),
            "inner"
        ).select("new.*")
    )
    
    df_subsystems_to_close = df_subsystems_active.alias("existing").join(
        df_subsystems_changed.alias("new"),
        F.col("existing.SubsystemCode") == F.col("new.SubsystemCode"),
        "inner"
    ).select(F.col("existing.*")).withColumn("end_date", F.date_sub(current_date, 1)).withColumn(
        "is_active", F.lit(False)
    )
    
    df_subsystems_final = df_subsystems_existing.filter(F.col("is_active") == False).union(
        df_subsystems_to_close
    ).union(df_subsystems_changed)
    
except:
    df_subsystems_final = df_subsystems_new

df_subsystems_final.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.{DIM_SUBSYSTEM}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. dim_component (SCD Type 2)

# COMMAND ----------

df_components = df_bronze.select(
    "DescriptiveCode",
    "ComponentDescription",
    "SubsystemCode",
).distinct().filter(
    F.col("DescriptiveCode").isNotNull() & (F.col("DescriptiveCode") != "")
)

df_components_new = df_components.withColumn("start_date", current_date).withColumn(
    "end_date", F.lit(None).cast(DateType())
).withColumn("is_active", F.lit(True)).withColumn(
    "surrogate_key", F.monotonically_increasing_id()
)

try:
    df_components_existing = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{DIM_COMPONENT}")
    df_components_active = df_components_existing.filter(F.col("is_active") == True)
    
    df_components_changed = df_components_new.alias("new").join(
        df_components_active.alias("existing"),
        F.col("new.DescriptiveCode") == F.col("existing.DescriptiveCode"),
        "left_anti"
    ).union(
        df_components_new.alias("new").join(
            df_components_active.alias("existing"),
            (F.col("new.DescriptiveCode") == F.col("existing.DescriptiveCode")) &
            (
                (F.col("new.ComponentDescription") != F.coalesce(F.col("existing.ComponentDescription"), F.lit(""))) |
                (F.col("new.SubsystemCode") != F.coalesce(F.col("existing.SubsystemCode"), F.lit("")))
            ),
            "inner"
        ).select("new.*")
    )
    
    df_components_to_close = df_components_active.alias("existing").join(
        df_components_changed.alias("new"),
        F.col("existing.DescriptiveCode") == F.col("new.DescriptiveCode"),
        "inner"
    ).select(F.col("existing.*")).withColumn("end_date", F.date_sub(current_date, 1)).withColumn(
        "is_active", F.lit(False)
    )
    
    df_components_final = df_components_existing.filter(F.col("is_active") == False).union(
        df_components_to_close
    ).union(df_components_changed)
    
except:
    df_components_final = df_components_new

df_components_final.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.{DIM_COMPONENT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Fact Table: fact_work_orders

# COMMAND ----------

# Get surrogate keys from dimensions (active records only)
df_dim_building = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{DIM_BUILDING}").filter(
    F.col("is_active") == True
).select(
    "BuildingID", "surrogate_key"
).withColumnRenamed("surrogate_key", "building_surrogate_key")

df_dim_system = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{DIM_SYSTEM}").filter(
    F.col("is_active") == True
).select(
    "SystemCode", "surrogate_key"
).withColumnRenamed("surrogate_key", "system_surrogate_key")

df_dim_subsystem = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{DIM_SUBSYSTEM}").filter(
    F.col("is_active") == True
).select(
    "SubsystemCode", "surrogate_key"
).withColumnRenamed("surrogate_key", "subsystem_surrogate_key")

df_dim_component = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{DIM_COMPONENT}").filter(
    F.col("is_active") == True
).select(
    "DescriptiveCode", "surrogate_key"
).withColumnRenamed("surrogate_key", "component_surrogate_key")

# Create fact table with foreign keys
df_fact = df_bronze.select(
    "WOID",
    "WODescription",
    "WOPriority",
    "WOStartDate",
    "WOEndDate",
    "WODuration",
    "work_type",
    "LaborCost",
    "MaterialCost",
    "OtherCost",
    "TotalCost",
    "LaborHours",
    "BuildingID",
    "SystemCode",
    "SubsystemCode",
    "DescriptiveCode",
    "MinTemp.(°C)",
    "MaxTemp.(°C)",
    "Atmospheric pressure(hPa)",
    "Humidity(%)",
    "WindSpeed(m/s)",
    "Precipitation(mm)",
    "_ingest_ts",
).join(df_dim_building, "BuildingID", "left").join(
    df_dim_system, "SystemCode", "left"
).join(df_dim_subsystem, "SubsystemCode", "left").join(
    df_dim_component, "DescriptiveCode", "left"
)

# Write fact table
df_fact.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.{FACT_WO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Created:
# MAGIC - **Fact table**: `fact_work_orders` (with foreign keys to dimensions)
# MAGIC - **Dimension tables** (SCD Type 2):
# MAGIC   - `dim_building` (start_date, end_date, is_active)
# MAGIC   - `dim_system` (start_date, end_date, is_active)
# MAGIC   - `dim_subsystem` (start_date, end_date, is_active)
# MAGIC   - `dim_component` (start_date, end_date, is_active)
# MAGIC
# MAGIC **SCD Type 2 logic**: When dimension attributes change, old records are closed (end_date set, is_active=False) and new records are created (start_date=current, end_date=NULL, is_active=True).
