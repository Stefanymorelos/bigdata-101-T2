# Databricks notebook source
# MAGIC %md
# MAGIC # Silver streaming
# MAGIC
# MAGIC Lee en streaming desde la tabla de Bronze streaming (`T_STREAM_BRONZE`)
# MAGIC y aplica las mismas reglas de limpieza que el Silver batch. Una tabla Delta
# MAGIC de UC es streaming-source nativo: cada commit nuevo en Bronze se propaga
# MAGIC automáticamente.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

bronze_stream = spark.readStream.table(T_STREAM_BRONZE)

# COMMAND ----------

cleaned = (
    bronze_stream
    .withColumn(
        "trip_duration_min",
        (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60.0,
    )
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("fare_amount") >= 0)
    .filter(F.col("total_amount") > 0)
    .filter(F.col("passenger_count").between(1, 8))
    .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    .filter(F.col("trip_duration_min").between(1, 360))
)

# COMMAND ----------

stream_query = (
    cleaned.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver_trips_stream")
    .option("mergeSchema", "true")
    .trigger(processingTime="1 minute")
    .queryName("silver_trips_stream")
    .toTable(T_STREAM_SILVER)
)
