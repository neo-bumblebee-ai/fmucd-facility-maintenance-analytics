# Orchestration — Databricks Job

Create a **Databricks Job** (Workflows) to run the medallion pipeline.

## Task order

| Order | Notebook | Description |
|-------|----------|-------------|
| 1 | `01_bronze_ingest` | Ingest FMUCD CSV → Bronze |
| 2 | `02_silver_cleanse` | Cleanse, dedupe → Silver |
| 3 | `02b_data_quality` | Data quality checks on Silver |
| 4 | `03_gold_aggregates` | Build Gold tables, OPTIMIZE/ZORDER |
| 5 | `04_ml_training` | (Optional) Train model, MLflow |
| 6 | `05_ml_scoring` | (Optional) Batch scoring |
| 7 | `06_dashboard_sql` | (Optional) Or use DBSQL/Lakeview on Gold |

## Steps

1. **Workflows → Create Job**
2. Add **Task** per notebook; set **Notebook path** to repo path, e.g.  
   `/Repos/<your-org>/<repo>/notebooks/01_bronze_ingest`
3. Chain tasks: each task depends on the previous (e.g. 02 → 01, 02b → 02, 03 → 02b).
4. Use a **Job Cluster** or **Existing All-Purpose Cluster** with Unity Catalog enabled.
5. **Schedule**: daily or on-demand; parameterize CSV path/date if needed.

## Parameters

Optionally add **Job parameters** (e.g. `fmucd_csv_path`) and pass them into the notebooks via `dbutils.widgets` or `spark.conf`.
