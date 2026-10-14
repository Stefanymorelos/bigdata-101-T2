# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Entrenamiento: predicción de duración de viaje (Spark MLlib)
# MAGIC
# MAGIC Usa un pipeline de Spark MLlib con `GBTRegressor` para predecir
# MAGIC la duración de un viaje en minutos. El modelo se loguea en MLflow.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

import mlflow
import mlflow.spark
from mlflow.models.signature import infer_signature
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar features y split temporal

# COMMAND ----------

features_df = spark.table(T_GOLD_ML_FEATURES)

# Muestreo del 5 % para agilizar el entrenamiento en clase
sample = features_df.sample(fraction=0.05, seed=42)

# Split temporal con hold-out de 7 días reservado para inferencia
max_date = sample.agg(F.max("pickup_date")).first()[0]
train_cutoff    = F.date_sub(F.lit(max_date), 30)
test_cutoff     = F.date_sub(F.lit(max_date), 7)

train_df = sample.filter(F.col("pickup_date") < train_cutoff)
test_df  = sample.filter(
    (F.col("pickup_date") >= train_cutoff) & (F.col("pickup_date") < test_cutoff)
)

print(f"Train: {train_df.count():,} | Test: {test_df.count():,}")
print(f"(Los últimos 7 días quedan como hold-out para 03_batch_inference)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Definir el pipeline de Spark MLlib

# COMMAND ----------

TARGET = "target_duration_min"
CATEGORICAL = ["pickup_borough", "dropoff_borough"]
NUMERIC = [
    "PULocationID", "DOLocationID", "pickup_hour", "pickup_dayofweek",
    "is_weekend", "is_rush_hour", "passenger_count", "trip_distance", "RatecodeID",
]

indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
    for c in CATEGORICAL
]

ohe = OneHotEncoder(
    inputCols=[f"{c}_idx" for c in CATEGORICAL],
    outputCols=[f"{c}_ohe" for c in CATEGORICAL],
    handleInvalid="keep",
)

assembler_inputs = [f"{c}_ohe" for c in CATEGORICAL] + NUMERIC
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features", handleInvalid="skip")

gbt = GBTRegressor(
    featuresCol="features",
    labelCol=TARGET,
    maxIter=100,
    maxDepth=5,
    stepSize=0.1,
    seed=42,
)

pipeline = Pipeline(stages=indexers + [ohe, assembler, gbt])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Entrenar y evaluar

# COMMAND ----------

with mlflow.start_run(run_name=f"{USER_INITIALS}_gbt_trip_duration") as run:

    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    evaluator_rmse = RegressionEvaluator(labelCol=TARGET, predictionCol="prediction", metricName="rmse")
    evaluator_mae  = RegressionEvaluator(labelCol=TARGET, predictionCol="prediction", metricName="mae")
    evaluator_r2   = RegressionEvaluator(labelCol=TARGET, predictionCol="prediction", metricName="r2")

    rmse = evaluator_rmse.evaluate(predictions)
    mae  = evaluator_mae.evaluate(predictions)
    r2   = evaluator_r2.evaluate(predictions)

    mlflow.log_metrics({"test_rmse": rmse, "test_mae": mae, "test_r2": r2})
    mlflow.log_params({
        "student": USER_INITIALS,
        "algorithm": "GBTRegressor",
        "maxIter": 100,
        "maxDepth": 5,
        "stepSize": 0.1,
    })

    input_cols = CATEGORICAL + NUMERIC
    sample_input = train_df.select(*input_cols).limit(5).toPandas()
    sample_output = predictions.select("prediction").limit(5).toPandas()
    signature = infer_signature(sample_input, sample_output)

    mlflow.spark.log_model(
        model,
        artifact_path="model",
        signature=signature,
        dfs_tmpdir=MLFLOW_DFS_TMPDIR,
    )

    run_id = run.info.run_id
    print(f"Run: {run_id}")
    print(f"RMSE: {rmse:.3f} | MAE: {mae:.3f} | R²: {r2:.4f}")

# Cuando corre como job (07-workflow), el task siguiente lee el run_id de aquí.
# Fuera de un job esta llamada no hace nada.
dbutils.jobs.taskValues.set(key="run_id", value=run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run ID para registrar el modelo

# COMMAND ----------

print("=" * 60)
print(f"RUN_ID: {run_id}")
print("=" * 60)
print("👉 Copia este run_id y pégalo en 02_register_model.py")
print("   (si corres el pipeline como job, se pasa solo entre tasks)")
