# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — ML Batch Scoring (Optional)
# MAGIC
# MAGIC - Load registered model from MLflow.
# MAGIC - Read Silver (or feature table); apply same transformations as training.
# MAGIC - Score and write to Gold (e.g. `gold_scored_work_orders`) or a dedicated table.

# COMMAND ----------

# import mlflow
# from mlflow.tracking import MlflowClient

# COMMAND ----------

# model_uri = "models:/fmucd_wo_risk/Production"
# model = mlflow.pyfunc.load_model(model_uri)
# df = spark.table(...)  # Silver or feature table
# pdf = df.toPandas()   # or use Spark ML model for native Spark scoring
# pdf["score"] = model.predict(pdf[feature_cols])
# result = spark.createDataFrame(pdf)
# result.write.format("delta").mode("overwrite").saveAsTable("...gold_scored_work_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC **TODO**: Wire to registered model, feature columns, and target Gold table. Run as Job step after `04_ml_training`.
