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
g
flowchart LR

    subgraph Source
        A[FMUCD CSV\n/Volumes/workspace/sor/fmucd]
    end

    subgraph Bronze["Bronze Layer (Delta)"]
        B1[Raw Ingest Table\nSchema Sanitized\nIngest TS, Batch ID]
    end

    subgraph Silver["Silver Layer (Delta)"]
        S1[dim_building\nSCD Type 2]
        S2[dim_system\nSCD Type 2]
        S3[fact_work_orders\nCleansed + Typed]
    end

    subgraph Gold["Gold Layer (Delta)"]
        G1[work_orders_enriched]
        G2[high_duration_risk_queue_ranked]
        G3[Ops Views\nTop N Queues]
    end

    subgraph ML["ML & Scoring"]
        M1[Feature Engineering]
        M2[Logistic Regression]
        M3[MLflow Tracking\nModel Registry]
    end

    subgraph Analytics["Analytics"]
        D1[Databricks SQL Views]
        D2[Ops Dashboard]
    end

    A --> B1
    B1 --> S1
    B1 --> S2
    B1 --> S3

    S1 --> G1
    S2 --> G1
    S3 --> G1

    G1 --> M1
    M1 --> M2
    M2 --> M3
    M2 --> G2

    G2 --> G3
    G3 --> D1
    D1 --> D2

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
