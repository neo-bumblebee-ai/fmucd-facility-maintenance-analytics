# Facility Maintenance Analytics (FMUCD) — Capstone

End-to-end **Databricks** capstone using the **Facility Management Unified Classification Database (FMUCD)** to build a medallion pipeline, optional ML (work-order risk), and a KPI dashboard.

## Real-life use case

**Problem**: Universities and large property portfolios face deferred maintenance, reactive vs preventive work, and work-order risk (cost overruns, long duration, critical systems).

**Solution**: Ingest FMUCD work-order and asset data → Bronze/Silver/Gold medallion → analytics + optional ML (e.g. high-cost / long-duration prediction) → SQL dashboard for facility managers and capital planners.

**Dataset**: **Facility Management Unified Classification Database (FMUCD)** — uploaded to Databricks Volume at `/Volumes/workspace/sor/fmucd/`. University campus (Canada, Nova Scotia): work orders, buildings, systems (HVAC, Electrical, Plumbing, Fire, etc.), costs, labor, weather.

## Capstone checklist and plan

- **[CAPSTONE_CHECKLIST.md](CAPSTONE_CHECKLIST.md)** — Tick off all PDF requirements (Days 15–21, minimum requirements, tech stack, optional stand-out items).
- **[docs/CAPSTONE_PLAN.md](docs/CAPSTONE_PLAN.md)** — Problem statement, success metrics, FMUCD overview, architecture, medallion design, business rules, Unity Catalog, orchestration.
- **[docs/WORKFLOW_JOB.md](docs/WORKFLOW_JOB.md)** — How to create a Databricks Job to run the pipeline.
- **[docs/CURSOR_DATABRICKS.md](docs/CURSOR_DATABRICKS.md)** — Connect Cursor to Databricks (VS Code extension).

## Architecture (high-level)

```
FMUCD CSV → Bronze (Delta) → Silver (Delta) → Gold (Delta)
                ↓                  ↓              ↓
         Raw ingest        Clean, validate   Aggregates (building,
         _ingest_ts        dedupe, work_type  system, PPM/UPM, KPIs)
                                                      ↓
                                    Analytics (SQL / dashboard) + optional ML (MLflow)
```

- **Unity Catalog**: `fmucd_capstone` catalog; `bronze`, `silver`, `gold` schemas.
- **Orchestration**: Databricks Job (see `docs/WORKFLOW_JOB.md`).

## First steps

1. Open **[CAPSTONE_CHECKLIST.md](CAPSTONE_CHECKLIST.md)** and use it to track progress.
2. Read **[docs/CAPSTONE_PLAN.md](docs/CAPSTONE_PLAN.md)** for the problem statement, FMUCD schema, and architecture.
3. Add this repo to **Databricks Repos**. The FMUCD CSV is already in Volume at `/Volumes/workspace/sor/fmucd/`.
4. Create the Unity Catalog catalog/schemas (or run `01_bronze_ingest`; it runs `CREATE … IF NOT EXISTS`).
5. Run notebooks **01 → 02 → 02b → 03** in order, then **06** for dashboard SQL. Optionally add **04** and **05** for ML.
6. Create a **Databricks Job** per [docs/WORKFLOW_JOB.md](docs/WORKFLOW_JOB.md) to orchestrate the pipeline.

## Setup

### 1. Repo and data

- Clone or add this repo to **Databricks Repos**.
- **Data source**: FMUCD CSV is already uploaded to Databricks Volume at `/Volumes/workspace/sor/fmucd/Facility Management Unified Classification Database (FMUCD).csv`
  - The Bronze ingestion notebook reads directly from this Volume path
  - If the path changes, update `FMUCD_CSV_PATH` in `notebooks/01_bronze_ingest.py`

### 2. Unity Catalog

- Create catalog `fmucd_capstone` and schemas `bronze`, `silver`, `gold` (notebooks run `CREATE … IF NOT EXISTS`).
- Attach a cluster with **Unity Catalog** enabled and access to that catalog.

### 3. Dependencies

- **Databricks**: Use a Delta/Spark runtime; MLflow is built-in. For local dev:
  ```bash
  pip install -r requirements.txt
  ```

### 4. Run the pipeline

**Notebooks** (in order):

1. `notebooks/01_bronze_ingest` — Read CSV from Volume → Ingest to Bronze.
2. `notebooks/02_silver_cleanse` — Cleanse → Silver.
3. `notebooks/02b_data_quality` — Data quality checks.
4. `notebooks/03_gold_aggregates` — Gold tables, OPTIMIZE/ZORDER.
5. `notebooks/04_ml_training` — (Optional) MLflow training.
6. `notebooks/05_ml_scoring` — (Optional) Batch scoring.
7. `notebooks/06_dashboard_sql` — Example SQL for **Databricks SQL** or **Lakeview** dashboard.

**Or** run via a **Databricks Job** as in `docs/WORKFLOW_JOB.md`.

### 5. Config

- Update `conf/config.yaml` and notebook variables (`CATALOG`, `FMUCD_CSV_PATH`, etc.) for your workspace if needed.

## Project layout

```
├── CAPSTONE_CHECKLIST.md    # Capstone tick-list
├── README.md
├── requirements.txt
├── conf/
│   └── config.yaml          # Catalog, schemas, tables, paths
├── data/                    # Optional local data
├── docs/
│   ├── CAPSTONE_PLAN.md     # Plan, architecture, FMUCD
│   ├── WORKFLOW_JOB.md      # Job setup
│   └── Databricks_14_Days_AI_Challenge.pdf
│   # Note: FMUCD.csv is in Databricks Volume at /Volumes/workspace/sor/fmucd/ (not in repo)
├── notebooks/
│   ├── 01_bronze_ingest
│   ├── 02_silver_cleanse
│   ├── 02b_data_quality
│   ├── 03_gold_aggregates
│   ├── 04_ml_training
│   ├── 05_ml_scoring
│   └── 06_dashboard_sql
└── screenshots/             # For dashboard/demo screenshots
```

## Findings and next steps

- Document **key findings** (e.g. top costly systems, PPM vs UPM mix, high-FCI buildings) in `docs/FINDINGS.md` or in the README as you complete the capstone.
- Add **architecture diagram** (draw.io, Mermaid, etc.) and link from README.
- **Demo**: Dashboard screenshots, short video, or live notebook walkthrough.

## License

See [LICENSE](LICENSE).
