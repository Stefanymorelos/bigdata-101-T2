# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Batch inference con el modelo registrado (Spark MLlib)
# MAGIC
# MAGIC Aplica el modelo registrado en UC sobre un hold-out temporal de los
# MAGIC últimos 7 días de ML features.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.sql import functions as F

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar el modelo desde Unity Catalog

# COMMAND ----------

model_uri = f"models:/{MODEL_NAME}@challenger"
model = mlflow.spark.load_model(model_uri, dfs_tmpdir=MLFLOW_DFS_TMPDIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Seleccionar datos recientes (hold-out de 7 días)

# COMMAND ----------

features_df = spark.table(T_GOLD_ML_FEATURES)

max_date = features_df.agg(F.max("pickup_date")).first()[0]
recent = features_df.filter(
    F.col("pickup_date") >= F.date_sub(F.lit(max_date), 7)
)
print(f"Filas a predecir: {recent.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Predecir y guardar

# COMMAND ----------

predictions = (
    model.transform(recent)
    .withColumnRenamed("prediction", "predicted_duration_min")
    .withColumn("prediction_error_min",
                F.col("target_duration_min") - F.col("predicted_duration_min"))
    .withColumn("predicted_at", F.current_timestamp())
)

output_cols = [
    "pickup_date", "pickup_ts", "PULocationID", "DOLocationID",
    "pickup_borough", "dropoff_borough", "trip_distance",
    "target_duration_min", "predicted_duration_min",
    "prediction_error_min", "predicted_at",
]

(
    predictions.select(*output_cols)
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_ML_PREDICTIONS)
)

print(f"✓ Predicciones escritas en {T_ML_PREDICTIONS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Métricas por día

# COMMAND ----------

preds = spark.table(T_ML_PREDICTIONS)

display(
    preds
    .groupBy("pickup_date")
    .agg(
        F.count("*").alias("predictions"),
        F.round(F.avg("prediction_error_min"), 2).alias("avg_error"),
        F.round(F.sqrt(F.avg(F.pow("prediction_error_min", 2))), 2).alias("rmse"),
        F.round(F.avg(F.abs("prediction_error_min")), 2).alias("mae"),
    )
    .orderBy("pickup_date")
)
