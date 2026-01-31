# FMUCD Databricks Capstone – Completion Checklist

## Project Overview
End-to-end Databricks pipeline for Facility Maintenance analytics using FMUCD data.
Implements Medallion architecture, ML-based risk scoring, and operational dashboards.

---

## ✅ Minimum Requirements

- ✅ Data Architecture (Bronze → Silver → Gold)
- ✅ Delta Lake tables with ACID guarantees
- ✅ Complex transformations & business rules
- ⬜ Orchestration (Databricks Workflow – scheduled next)
- ✅ Governance (Unity Catalog, schemas, naming)
- ⬜ Analytics Dashboard (views created; DBSQL dashboard pending)
- ✅ ML Component (training, MLflow tracking, scoring)
- ✅ Documentation (README, plan, findings, workflow)

---

## 📅 Days 15–16: Planning & Setup
- ✅ Defined real-world problem (maintenance duration risk)
- ✅ Dataset explored (FMUCD CSV)
- ✅ Architecture designed
- ✅ Unity Catalog structure created
- ✅ Repo & notebook structure created

## 📅 Days 17–21: Implementation
- ✅ Bronze ingestion with schema sanitization
- ✅ Silver cleansing + SCD Type-2 dimensions
- ✅ Silver fact table built
- ✅ Gold aggregates and enriched dataset
- ✅ ML model trained & logged to MLflow
- ✅ Full dataset scoring completed
- ✅ Percentile-based risk ranking implemented
- ✅ Analytics Views
- ⬜ Workflow scheduling

---

## Final Status
**Capstone technically complete.**