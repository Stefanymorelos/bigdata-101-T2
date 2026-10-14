# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: tabla estática de zonas
# MAGIC
# MAGIC Lee el CSV de zonas que el profesor dejó en el landing y lo persiste
# MAGIC en la tabla administrada de Unity Catalog `T_BRONZE_ZONES`.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

zones_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{ZONES_VOLUME_PATH}/taxi_zone_lookup.csv")
)

zones_df.printSchema()
zones_df.display()

# COMMAND ----------

(
    zones_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_BRONZE_ZONES)
)

print(f"✓ Zones escritas en {T_BRONZE_ZONES}")

# COMMAND ----------

zones_check = spark.table(T_BRONZE_ZONES)
print(f"Total zonas: {zones_check.count()}")
print(f"Boroughs distintos: {zones_check.select('Borough').distinct().count()}")
