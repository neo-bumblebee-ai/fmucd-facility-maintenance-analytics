# FMUCD Facility Maintenance Analytics – Databricks Capstone

## Problem Statement
Facilities teams manage millions of work orders annually.
Some work orders become long-running, costly, and operationally disruptive.
There is no systematic way to prioritize which work orders need early attention.

## Solution
An end-to-end Databricks pipeline that:
- Ingests FMUCD data into Delta Lake
- Models clean facts and SCD-2 dimensions
- Trains an ML model to predict long-duration risk
- Scores all work orders
- Ranks them into actionable risk buckets (HIGH / MEDIUM / LOW)

## Architecture

```mermaid
flowchart LR

  subgraph Source
    A["FMUCD CSV<br/>/Volumes/workspace/sor/fmucd"]
  end

  subgraph Bronze["Bronze (Delta)"]
    B1["bronze.bronze_fmucd_raw<br/>raw ingest + schema sanitization<br/>ingestion_ts, batch_id"]
  end

  subgraph Silver["Silver (Delta)"]
    S1["silver.dim_building (SCD2)<br/>start_date, end_date, active_flag<br/>load_date, last_updated_date"]
    S2["silver.dim_system (SCD2)<br/>start_date, end_date, active_flag<br/>load_date, last_updated_date"]
    S3["silver.fact_work_orders<br/>cleansed + typed + business rules"]
  end

  subgraph Gold["Gold (Delta)"]
    G1["gold.work_orders_enriched"]
    G2["gold.high_duration_risk_queue"]
    G3["gold.high_duration_risk_queue_ranked<br/>percentile buckets"]
    G4["gold.v_ops_queue_top500 / v_risk_by_building / v_risk_by_system"]
  end

  subgraph ML["ML + MLflow"]
    M1["Feature engineering"]
    M2["Logistic Regression"]
    M3["MLflow tracking (runs)"]
  end

  subgraph Analytics["Analytics"]
    D1["Databricks SQL views"]
    D2["DBSQL dashboard (optional)"]
  end

  A --> B1
  B1 --> S1
  B1 --> S2
  B1 --> S3

  S1 --> G1
  S2 --> G1
  S3 --> G1

  G1 --> M1 --> M2 --> M3
  M2 --> G2 --> G3 --> G4
  G4 --> D1 --> D2
  ```

**Bronze**
- Raw FMUCD CSV ingested from Databricks Volume
- Schema sanitization for Delta compatibility

**Silver**
- Cleaned fact table (`fact_work_orders`)
- SCD-2 dimensions (building, system, component)

**Gold**
- Aggregated KPIs
- Enriched work orders
- ML risk scoring output
- Ranked operational queues

## Data Source
FMUCD CSV uploaded to Databricks Volume: 
/Volumes/workspace/sor/fmucd/Facility Management Unified Classification Database (FMUCD).csv

## Key Outputs
- `gold.work_orders_enriched`
- `gold.high_duration_risk_queue`
- `gold.high_duration_risk_queue_ranked`
- `gold.v_ops_queue_top500`
- `gold.v_risk_by_building`
- `gold.v_risk_by_system`

## Machine Learning
- Model: Logistic Regression
- Label: Top 10% duration within (system_code, wo_priority)
- Metric: AUC ≈ 0.56
- Operationalized using percentile-based ranking

## How to Run
Execution is orchestrated via Databricks Workflows.
See `docs/WORKFLOW_JOB.md` for task order and dependencies.

## Repository Structure
```text
fmucd-facility-maintenance-analytics/
├─ notebooks/
│  ├─ 01_bronze_ingest.py
│  ├─ 02_silver_cleanse.py
│  ├─ 02b_data_quality.py
│  ├─ 03_gold_aggregates.py
│  ├─ 04_ml_training.py
│  ├─ 05_ml_scoring.py
│  └─ 06_dashboard_sql.py
├─ docs/
│  ├─ CAPSTONE_PLAN.md
│  ├─ FINDINGS.md
│  ├─ WORKFLOW_JOB.md
│  └─ CURSOR_DATABRICKS.md
├─ conf/
│  └─ config.yaml
├─ screenshots/
├─ sql/
├─ .gitignore
├─ CAPSTONE_CHECKLIST.md
├─ databricks.yml
├─ LICENSE
├─ README.md
└─ requirements.txt
