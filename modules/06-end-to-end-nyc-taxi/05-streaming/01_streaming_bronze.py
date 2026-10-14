# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze streaming: Auto Loader en modo continuo
# MAGIC
# MAGIC Mismo concepto que el Bronze batch, pero con trigger de procesamiento
# MAGIC continuo. Cada 30 segundos Auto Loader revisa el landing zone y
# MAGIC procesa lo que haya nuevo. Escribe en la tabla administrada de Unity
# MAGIC Catalog `T_STREAM_BRONZE`; solo el checkpoint y el schema location de
# MAGIC Auto Loader quedan en el Volume, porque son archivos de estado, no datos.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/schema_bronze_stream")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(LANDING_TRIPS_PATH)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------

stream_query = (
    raw_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze_trips_stream")
    .option("mergeSchema", "true")
    .trigger(processingTime="30 seconds")
    .queryName("bronze_yellow_trips_stream")
    .toTable(T_STREAM_BRONZE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC El stream está corriendo. Ir a la pestaña **Structured Streaming** del notebook
# MAGIC para ver throughput, batch size y latencia en vivo.
# MAGIC
# MAGIC Para detenerlo:
# MAGIC ```python
# MAGIC stream_query.stop()
# MAGIC ```
