# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: viajes enriquecidos con zonas y features temporales

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# 01_silver_trips_clean ya trae las columnas de zona; se quitan antes del join
# para no duplicarlas (Delta rechaza columnas con el mismo nombre).
_ZONE_COLS = ["pickup_borough", "pickup_zone", "pickup_service_zone",
              "dropoff_borough", "dropoff_zone", "dropoff_service_zone"]
trips = spark.table(T_SILVER_TRIPS)
trips = trips.drop(*[c for c in _ZONE_COLS if c in trips.columns])
zones = spark.table(T_BRONZE_ZONES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Broadcast join con zonas

# COMMAND ----------

zones_pickup = zones.select(
    F.col("LocationID").alias("PULocationID"),
    F.col("Borough").alias("pickup_borough"),
    F.col("Zone").alias("pickup_zone"),
    F.col("service_zone").alias("pickup_service_zone"),
)

zones_dropoff = zones.select(
    F.col("LocationID").alias("DOLocationID"),
    F.col("Borough").alias("dropoff_borough"),
    F.col("Zone").alias("dropoff_zone"),
    F.col("service_zone").alias("dropoff_service_zone"),
)

enriched = (
    trips
    .join(F.broadcast(zones_pickup),  on="PULocationID", how="left")
    .join(F.broadcast(zones_dropoff), on="DOLocationID", how="left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Features temporales y económicas

# COMMAND ----------

enriched = (
    enriched
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("pickup_dayofweek", F.dayofweek("tpep_pickup_datetime"))
    .withColumn("is_weekend", F.col("pickup_dayofweek").isin(1, 7))
    .withColumn("is_rush_hour",
        F.col("pickup_hour").between(7, 9) | F.col("pickup_hour").between(17, 19))
    .withColumn("tip_rate",
        F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount"))
        .otherwise(F.lit(0.0)))
    .withColumn("cost_per_mile",
        F.when(F.col("trip_distance") > 0, F.col("total_amount") / F.col("trip_distance"))
        .otherwise(F.lit(None)))
    .withColumn("avg_speed_mph",
        F.when(F.col("trip_duration_min") > 0,
               F.col("trip_distance") / (F.col("trip_duration_min") / 60.0))
        .otherwise(F.lit(None)))
)

# COMMAND ----------

(
    enriched.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("pickup_date")
    .saveAsTable(T_SILVER_TRIPS_ENRICHED)
)

print(f"✓ Silver enriched escrito en {T_SILVER_TRIPS_ENRICHED}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación

# COMMAND ----------

enriched_check = spark.table(T_SILVER_TRIPS_ENRICHED)

display(
    enriched_check
    .filter(F.col("pickup_borough").isNotNull())
    .groupBy("pickup_borough")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare"),
        F.round(F.avg("tip_rate") * 100, 1).alias("avg_tip_pct"),
    )
    .orderBy(F.desc("trips"))
)