# Databricks notebook source
# MAGIC %md
# MAGIC # Reporte de calidad de datos

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

rejected = spark.table(T_SILVER_REJECTED)
bronze = spark.table(T_BRONZE_TRIPS)
silver = spark.table(T_SILVER_TRIPS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detalle de rechazos por regla

# COMMAND ----------

display(
    rejected.agg(
        F.count("*").alias("total_rejected"),
        F.sum((~F.col("valid_distance")).cast("int")).alias("fail_distance"),
        F.sum((~F.col("valid_fare")).cast("int")).alias("fail_fare"),
        F.sum((~F.col("valid_total")).cast("int")).alias("fail_total"),
        F.sum((~F.col("valid_passengers")).cast("int")).alias("fail_passengers"),
        F.sum((~F.col("valid_dropoff")).cast("int")).alias("fail_dropoff"),
        F.sum((~F.col("valid_duration")).cast("int")).alias("fail_duration"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de pipeline

# COMMAND ----------

bronze_total = bronze.count()
silver_valid = silver.count()
silver_rejected = rejected.count()

print(f"Bronze total:    {bronze_total:>12,}")
print(f"Silver válidos:  {silver_valid:>12,}  ({100.0 * silver_valid / bronze_total:.2f}%)")
print(f"Silver rechazos: {silver_rejected:>12,}  ({100.0 * silver_rejected / bronze_total:.2f}%)")
