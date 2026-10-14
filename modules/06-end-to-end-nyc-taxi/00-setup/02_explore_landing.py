# Databricks notebook source
# MAGIC %md
# MAGIC # Exploración del landing
# MAGIC
# MAGIC El landing es un **Volume de Unity Catalog** donde el profesor depositó
# MAGIC los archivos Parquet mensuales de NYC TLC Yellow Taxi.
# MAGIC
# MAGIC Los archivos viven en `/Volumes/nytaxi_landing/raw/files/` y todos los
# MAGIC estudiantes tienen permiso de lectura — sin SAS tokens ni access keys.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Qué archivos hay en el landing

# COMMAND ----------

for f in dbutils.fs.ls(LANDING_TRIPS_PATH):
    print(f"  {f.name:40s} {f.size/1024/1024:>8.2f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Esquema de los parquets

# COMMAND ----------

df = read_yellow_trips(spark)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Conteo y rango de fechas

# COMMAND ----------

from pyspark.sql import functions as F

df.agg(
    F.count("*").alias("total_rows"),
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Muestra de 10 filas

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Estadísticas de columnas numéricas

# COMMAND ----------

df.select(
    "passenger_count", "trip_distance", "fare_amount",
    "tip_amount", "total_amount",
).describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Señales de datos sucios
# MAGIC
# MAGIC Cosas que no pueden ser ciertas y que nos servirán para diseñar
# MAGIC las reglas de limpieza en Silver.

# COMMAND ----------

df.select(
    F.sum(F.when(F.col("trip_distance") <= 0, 1).otherwise(0)).alias("distance_zero_or_neg"),
    F.sum(F.when(F.col("fare_amount") < 0, 1).otherwise(0)).alias("negative_fare"),
    F.sum(F.when(F.col("passenger_count") == 0, 1).otherwise(0)).alias("zero_passengers"),
    F.sum(F.when(F.col("passenger_count").isNull(), 1).otherwise(0)).alias("null_passengers"),
    F.sum(F.when(F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"), 1).otherwise(0)).alias("dropoff_before_pickup"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Inspección del lookup de zonas

# COMMAND ----------

zones = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{LANDING_ZONES_PATH}/taxi_zone_lookup.csv")
)
zones.display()
print(f"Total zones: {zones.count()}")
