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
notebooks/
01_bronze_ingest
02_silver_cleanse
02b_data_quality
03_gold_aggregates
04_ml_training
05_ml_scoring
06_dashboard_sql

docs/
CAPSTONE_PLAN.md
FINDINGS.md
WORKFLOW_JOB.md
CURSOR_DATABRICKS.md