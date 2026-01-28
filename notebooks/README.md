# Notebooks — Capstone Pipeline

Run in order (or via a single Databricks Job):

1. **01_bronze_ingest** — Ingest FMUCD CSV into Bronze Delta table.
2. **02_silver_cleanse** — Cleanse, validate, deduplicate → Silver tables.
3. **02b_data_quality** — Data quality checks on Silver (nulls, duplicates, value ranges).
4. **03_gold_aggregates** — Build Gold aggregates (by building, system, PPM/UPM, KPIs).
5. **04_ml_training** — (Optional) Train model, log to MLflow, register.
6. **05_ml_scoring** — (Optional) Batch score work orders from Silver/Gold.
7. **06_dashboard_sql** — Example SQL / dashboard logic; use with Databricks SQL or Lakeview.

Copy these into Databricks Repos or create corresponding `.ipynb` notebooks. Update `conf/config.yaml` with your catalog, schemas, and paths.
