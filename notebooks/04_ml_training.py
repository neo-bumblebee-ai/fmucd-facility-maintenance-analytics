# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — ML Training (Optional)
# MAGIC
# MAGIC - Build feature table from Silver (and Gold) e.g. SystemCode, BuildingID, work_type, FCI, costs, weather.
# MAGIC - Define target: e.g. `high_cost` (TotalCost > p95) or `WODuration` regression.
# MAGIC - Train classifier/regressor; log to MLflow; register best model.

# COMMAND ----------

# Uncomment and use when running on Databricks with MLflow.
# import mlflow
# import mlflow.sklearn  # or mlflow.spark for Spark ML
# from sklearn.model_selection import train_test_split
# from sklearn.ensemble import RandomForestClassifier

# COMMAND ----------

CATALOG = "fmucd_capstone"
SILVER_SCHEMA = "silver"
TABLE_WO = "silver_work_orders"
EXPERIMENT_NAME = "/Shared/fmucd_capstone_ml"

# COMMAND ----------

# df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{TABLE_WO}")
# Prepare features: string index or one-hot SystemCode, work_type; numeric FCI, LaborCost, etc.
# Define target, e.g. (F.col("TotalCost") > percentile).cast("int").alias("high_cost")
# mlflow.set_experiment(EXPERIMENT_NAME)
# with mlflow.start_run():
#     model = RandomForestClassifier(...)
#     model.fit(X_train, y_train)
#     mlflow.log_metrics({"accuracy": ...})
#     mlflow.sklearn.log_model(model, "model")
#     mlflow.register_model("runs:/<run_id>/model", "fmucd_wo_risk")

# COMMAND ----------

# MAGIC %md
# MAGIC **TODO**: Implement feature prep, train/eval, MLflow logging, model registration. Use `05_ml_scoring` for batch scoring.
