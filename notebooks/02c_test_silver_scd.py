# Databricks notebook source
# MAGIC %md
# MAGIC # 02c — Test Silver Layer (Fact & Dimension Tables with SCD Type 2)
# MAGIC
# MAGIC Validation queries to verify:
# MAGIC - Fact table structure and foreign keys
# MAGIC - Dimension tables have SCD Type 2 columns (start_date, end_date, is_active)
# MAGIC - Only one active record per dimension key
# MAGIC - Historical records are properly closed

# COMMAND ----------

CATALOG = "fmucd_capstone"
SILVER_SCHEMA = "silver"
FACT_WO = "fact_work_orders"
DIM_BUILDING = "dim_building"
DIM_SYSTEM = "dim_system"
DIM_SUBSYSTEM = "dim_subsystem"
DIM_COMPONENT = "dim_component"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Check Fact Table Structure

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fact table row count and sample
# MAGIC SELECT COUNT(*) as fact_row_count
# MAGIC FROM fmucd_capstone.silver.fact_work_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample fact records with foreign keys
# MAGIC SELECT 
# MAGIC   WOID,
# MAGIC   WODescription,
# MAGIC   work_type,
# MAGIC   TotalCost,
# MAGIC   BuildingID,
# MAGIC   building_surrogate_key,
# MAGIC   SystemCode,
# MAGIC   system_surrogate_key
# MAGIC FROM fmucd_capstone.silver.fact_work_orders
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Check Dimension Tables - SCD Type 2 Structure

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dim_building: Check SCD Type 2 columns
# MAGIC SELECT 
# MAGIC   BuildingID,
# MAGIC   BuildingName,
# MAGIC   start_date,
# MAGIC   end_date,
# MAGIC   is_active,
# MAGIC   surrogate_key
# MAGIC FROM fmucd_capstone.silver.dim_building
# MAGIC ORDER BY BuildingID, start_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify only one active record per BuildingID
# MAGIC SELECT 
# MAGIC   BuildingID,
# MAGIC   COUNT(*) as active_count
# MAGIC FROM fmucd_capstone.silver.dim_building
# MAGIC WHERE is_active = true
# MAGIC GROUP BY BuildingID
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for buildings with historical records (SCD Type 2 working)
# MAGIC SELECT 
# MAGIC   BuildingID,
# MAGIC   COUNT(*) as total_records,
# MAGIC   SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END) as active_records,
# MAGIC   SUM(CASE WHEN is_active = false THEN 1 ELSE 0 END) as historical_records
# MAGIC FROM fmucd_capstone.silver.dim_building
# MAGIC GROUP BY BuildingID
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY total_records DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dim_system: Check SCD Type 2
# MAGIC SELECT 
# MAGIC   SystemCode,
# MAGIC   SystemDescription,
# MAGIC   start_date,
# MAGIC   end_date,
# MAGIC   is_active
# MAGIC FROM fmucd_capstone.silver.dim_system
# MAGIC ORDER BY SystemCode, start_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dim_subsystem: Check SCD Type 2
# MAGIC SELECT 
# MAGIC   SubsystemCode,
# MAGIC   SubsystemDescription,
# MAGIC   SystemCode,
# MAGIC   start_date,
# MAGIC   end_date,
# MAGIC   is_active
# MAGIC FROM fmucd_capstone.silver.dim_subsystem
# MAGIC ORDER BY SubsystemCode, start_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- dim_component: Check SCD Type 2
# MAGIC SELECT 
# MAGIC   DescriptiveCode,
# MAGIC   ComponentDescription,
# MAGIC   SubsystemCode,
# MAGIC   start_date,
# MAGIC   end_date,
# MAGIC   is_active
# MAGIC FROM fmucd_capstone.silver.dim_component
# MAGIC ORDER BY DescriptiveCode, start_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify Foreign Key Relationships

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check fact table joins with dimensions (should have matching records)
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_fact_rows,
# MAGIC   COUNT(building_surrogate_key) as rows_with_building_fk,
# MAGIC   COUNT(system_surrogate_key) as rows_with_system_fk,
# MAGIC   COUNT(subsystem_surrogate_key) as rows_with_subsystem_fk,
# MAGIC   COUNT(component_surrogate_key) as rows_with_component_fk
# MAGIC FROM fmucd_capstone.silver.fact_work_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Summary Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dimension table row counts
# MAGIC SELECT 'dim_building' as table_name, COUNT(*) as total_rows, 
# MAGIC        SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END) as active_rows
# MAGIC FROM fmucd_capstone.silver.dim_building
# MAGIC UNION ALL
# MAGIC SELECT 'dim_system', COUNT(*), SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END)
# MAGIC FROM fmucd_capstone.silver.dim_system
# MAGIC UNION ALL
# MAGIC SELECT 'dim_subsystem', COUNT(*), SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END)
# MAGIC FROM fmucd_capstone.silver.dim_subsystem
# MAGIC UNION ALL
# MAGIC SELECT 'dim_component', COUNT(*), SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END)
# MAGIC FROM fmucd_capstone.silver.dim_component;

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected Results:**
# MAGIC - ✅ Fact table has rows with foreign keys to dimensions
# MAGIC - ✅ Each dimension has `start_date`, `end_date`, `is_active` columns
# MAGIC - ✅ Only one active record (`is_active = true`) per dimension key
# MAGIC - ✅ Historical records have `is_active = false` and `end_date` set
# MAGIC - ✅ Foreign keys in fact table match active dimension records
