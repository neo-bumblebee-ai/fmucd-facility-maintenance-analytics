# Databricks Workflow – FMUCD Capstone

## Job Name
fmucd_capstone_pipeline

## Task Order
1. 01_bronze_ingest
2. 02_silver_cleanse
3. 03_gold_aggregates
4. 04_ml_training
5. 05_ml_scoring
6. 06_dashboard_sql (optional)

## Dependencies
Each task depends on the previous task completing successfully.

## MLflow Integration
- `04_ml_training` stores run_id using taskValues
- `05_ml_scoring` retrieves run_id dynamically

## Parameters (optional)
- catalog: fmucd_capstone
- source_path: FMUCD CSV Volume path

## Notes
Workflow is designed to be idempotent and re-runnable.