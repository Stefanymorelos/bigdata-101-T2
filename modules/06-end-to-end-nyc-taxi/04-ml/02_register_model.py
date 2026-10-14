# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Registro del modelo en Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Widget para el `run_id`
# MAGIC
# MAGIC Corre esta celda primero para crear el widget. Luego pega en el
# MAGIC widget (arriba del notebook) el `run_id` que imprimió el notebook
# MAGIC `01_train_trip_duration`.

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Run ID del training")

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer el `run_id` del widget
# MAGIC
# MAGIC El modelo se registra en tu mismo schema (`MODEL_NAME`), así que no hace
# MAGIC falta crear nada antes: como dueño del schema ya tienes `CREATE MODEL`.

# COMMAND ----------

run_id = dbutils.widgets.get("run_id").strip()

# En un job, el task `train` publica el run_id como task value; el job se lo
# pasa a este widget con base_parameters: {run_id: "{{tasks.train.values.run_id}}"}.
# Si el widget llegó vacío, se intenta leer directamente el task value.
if not run_id:
    run_id = dbutils.jobs.taskValues.get(taskKey="train", key="run_id", default="", debugValue="")

assert run_id, "Pega el run_id del notebook 01_train_trip_duration en el widget 'run_id'."
print(f"Registrando modelo del run: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Registrar en UC

# COMMAND ----------

model_uri = f"runs:/{run_id}/model"
registered = mlflow.register_model(model_uri=model_uri, name=MODEL_NAME)
print(f"✓ Registrado {MODEL_NAME} versión {registered.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Asignar alias `challenger`

# COMMAND ----------

client = MlflowClient()
client.set_registered_model_alias(name=MODEL_NAME, alias="challenger", version=registered.version)
print(f"✓ Alias 'challenger' asignado a versión {registered.version}")
