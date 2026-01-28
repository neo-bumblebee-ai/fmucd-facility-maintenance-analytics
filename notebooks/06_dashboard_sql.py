# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Dashboard SQL Examples
# MAGIC
# MAGIC Example queries for **Databricks SQL** or **Lakeview** dashboard.
# MAGIC Create a DBSQL dashboard and add widgets: building, system, date range.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPIs
# MAGIC SELECT * FROM fmucd_capstone.gold.gold_kpis;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PPM vs UPM summary
# MAGIC SELECT work_type, wo_count, total_cost, labor_hours
# MAGIC FROM fmucd_capstone.gold.gold_ppm_upm_summary
# MAGIC ORDER BY total_cost DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top systems by cost
# MAGIC SELECT SystemCode, SystemDescription, work_type, wo_count, total_cost, labor_hours
# MAGIC FROM fmucd_capstone.gold.gold_wo_by_system
# MAGIC ORDER BY total_cost DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top buildings by cost
# MAGIC SELECT BuildingID, BuildingName, SystemCode, work_type, wo_count, total_cost, labor_hours
# MAGIC FROM fmucd_capstone.gold.gold_wo_by_building
# MAGIC WHERE BuildingID != 'Campus-level'
# MAGIC ORDER BY total_cost DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC **Dashboard**: Add filters (BuildingID, SystemCode, work_type), bar/line charts for cost and labor hours, KPI cards.
