# Capstone Plan — Facility Maintenance Analytics (FMUCD)

## Real-Life Use Case

**Domain**: Facility Management (University Campus)  
**Dataset**: Facility Management Unified Classification Database (FMUCD)  
**Problem**: Universities and large property portfolios struggle with **deferred maintenance**, reactive vs preventive work, and **work-order risk** (cost overruns, long duration, safety-critical systems). This project uses FMUCD to build an **end-to-end analytics and ML pipeline** that supports:

- **Prioritization**: Identify high-risk work orders (cost, duration, critical systems).
- **Spend optimization**: Shift from reactive (UPM) to preventive (PPM) where beneficial.
- **Actionable insights**: Dashboards and models for facility managers and capital planners.

---

## 1. Problem Statement

**What**: Build a Databricks-based pipeline that ingests FMUCD work-order and asset data, cleans and enriches it in a medallion architecture, and produces analytics + ML outputs (e.g. risk scoring, cost/duration predictors) for facility decision-making.

**Why**: Reduce unplanned spend, better allocate maintenance budget, and surface high-risk assets/buildings before failures.

**Success metrics**:

- Pipeline runs reproducibly (Bronze → Silver → Gold) with documented business rules.
- Data quality checks implemented (nulls, duplicates, valid code ranges).
- SQL/dashboard answers: top costly systems, PPM vs UPM mix, building-level deferred maintenance.
- (Optional) ML model (e.g. high-cost / long-duration predictor) registered in MLflow with clear metrics.

---

## 2. Dataset — FMUCD Overview

**File**: `docs/Facility Management Unified Classification Database (FMUCD).csv`

**Context**: University campus (Canada, Nova Scotia); work orders, buildings, systems, costs, weather.

| Category | Columns |
|----------|---------|
| **Org/Location** | UniversityID, Country, State/Province |
| **Building** | BuildingID, BuildingName, Size, Type, BuiltYear |
| **Condition / $** | FCI (facility condition index), CRV (current replacement value), DMC (deferred maintenance cost) |
| **Systems** | SystemCode, SystemDescription, SubsystemCode, SubsystemDescription, DescriptiveCode, ComponentDescription |
| **Work orders** | WOID, WODescription, WOPriority, WOStartDate, WOEndDate, WODuration, PPM/UPM, LaborCost, MaterialCost, OtherCost, TotalCost, LaborHours |
| **Weather** | MinTemp.(°C), MaxTemp.(°C), Atmospheric pressure(hPa), Humidity(%), WindSpeed(m/s), WindDegree, Precipitation(mm), Snow(mm), Cloudness(%) |

**Example systems**: D20 Plumbing, D30 HVAC, D40 Fire Protection, D50 Electrical, C30 Interior Finishes, E10/E20 Equipment/Furnishings.

**EDA focus**: Nulls (e.g. BuildingID for campus-level WOs), duplicates (same WOID, multiple rows), distributions of cost/duration, PPM vs UPM, FCI vs DMC.

---

## 3. Architecture (High-Level)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  INGESTION                                                                   │
│  FMUCD.csv (or cloud storage) → Databricks File Store / cloud volume         │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  BRONZE (Delta)                                                              │
│  Raw ingestion, schema infer/hints, _ingest_ts, source_file                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  SILVER (Delta)                                                              │
│  Cleansed, validated, deduplicated; business keys; data quality checks       │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  GOLD (Delta)                                                                │
│  Aggregations: by building, system, PPM/UPM, time; KPI tables                │
└─────────────────────────────────────────────────────────────────────────────┘
         │                                    │
         ▼                                    ▼
┌─────────────────────┐            ┌─────────────────────────────────────────┐
│  ANALYTICS           │            │  ML (optional)                          │
│  SQL / Dashboard     │            │  Features from Silver/Gold → MLflow     │
│  KPIs, drill-downs   │            │  Model registry, batch scoring          │
└─────────────────────┘            └─────────────────────────────────────────┘
         │                                    │
         └────────────────┬───────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  GOVERNANCE & ORCHESTRATION                                                  │
│  Unity Catalog (catalogs, schemas, tables, permissions)                      │
│  Databricks Jobs / Workflows: ingest → Bronze → Silver → Gold → ML           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Medallion Design

| Layer | Purpose | Key tables (examples) |
|-------|---------|------------------------|
| **Bronze** | Raw, append-only | `bronze_fmucd_raw` |
| **Silver** | Clean, conformed | `silver_work_orders`, `silver_buildings`, `silver_systems` (or single normalized table) |
| **Gold** | Business aggregates | `gold_wo_by_building`, `gold_wo_by_system`, `gold_ppm_upm_summary`, `gold_kpis` |

Use **Delta** for all; add `OPTIMIZE` and `ZORDER` where queries filter by `BuildingID`, `SystemCode`, `WOID`, or time.

---

## 5. Business Rules (Transformations)

- **Deduplication**: Deterministic dedup on `WOID` (+ optional natural key) before Silver.
- **Nulls**: Decide convention for missing `BuildingID` (e.g. “Campus-level”) and document.
- **PPM/UPM**: Standardize `PPM/UPM` → `work_type` (PPM vs UPM); use in Gold breakdowns.
- **Cost**: `TotalCost` = Labor + Material + Other; validate and optionally backfill from components.
- **Duration**: `WODuration` in days; flag negative or null; optional `WOEndDate` imputation for open WOs.
- **Risk**: (Gold/ML) Define “high risk” e.g. by `WOPriority`, `TotalCost` percentile, `SystemCode` (e.g. D40 Fire, D50 Electrical), or model score.

---

## 6. Orchestration

- **Workflow**: Single Job or multi-task Job: `Ingest → Bronze → Silver → Gold [→ ML]`.
- **Schedule**: Daily or on-demand; parametrize input path/date if needed.
- **Notebooks**: Separate notebooks (or staged steps) per layer for clarity and reuse.

---

## 7. Unity Catalog

- Create **catalog** (e.g. `fmucd_capstone`) and **schemas**: `bronze`, `silver`, `gold`.
- Store all Delta tables in these schemas; apply **grants** so only intended users/jobs can read/write.
- Document catalog/schema/table names in README and runbooks.

---

## 8. Analytics & Dashboard

- **SQL**: Reuse Gold tables for top-N queries, PPM vs UPM, building/system rollups, FCI vs DMC.
- **Dashboard**: Build in **Databricks SQL** or **Lakeview** (or external BI): KPIs, filters (building, system, date range), simple charts.

---

## 9. ML Component (Optional)

- **Target**: e.g. `high_cost` (binary) or `WODuration` (regression).
- **Features**: From Silver/Gold (system, building type, FCI, PPM/UPM, weather, etc.).
- **MLflow**: Log experiments, register best model, add **batch scoring** notebook/Job writing to Gold or a dedicated `scored_work_orders` table.

---

## 10. Repo Structure (Notebooks)

```
notebooks/
├── 01_bronze_ingest
├── 02_silver_cleanse
├── 03_gold_aggregates
├── 04_ml_training      # optional
├── 05_ml_scoring       # optional
├── 06_dashboard_sql    # or link to DBSQL/Lakeview)
conf/
├── config.yaml         # paths, catalog, schema, table names
```

---

## 11. Documentation Deliverables

- **README**: Setup, architecture diagram link, how to run Jobs, where FMUCD lives.
- **CAPSTONE_CHECKLIST**: Track all capstone requirements and week plan.
- **Findings**: Short “Key findings” doc (or section in README): insight examples, model performance, business recommendations.

---

*Use this plan alongside `CAPSTONE_CHECKLIST.md` to tick off tasks as you implement.*
