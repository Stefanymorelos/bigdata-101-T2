# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingesta de viajes desde el landing compartido
# MAGIC
# MAGIC Lee los Parquet mensuales del landing (`LANDING_VOLUME_PATH`) y los
# MAGIC persiste en la tabla administrada de Unity Catalog `T_BRONZE_TRIPS`.
# MAGIC Opera en modo **append** mes a mes, permitiendo cargas incrementales.
# MAGIC
# MAGIC **Idempotencia**: si un mes ya fue ingestado, se salta automáticamente.
# MAGIC
# MAGIC ## Uso
# MAGIC
# MAGIC Ajusta `MONTHS_TO_INGEST` para controlar qué meses se cargan.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros de ingesta

# COMMAND ----------

# Meses a ingerir en esta ejecución.
# Ajusta según lo que necesites cargar.
# El landing del curso tiene de 2023-01 a 2024-08. Para el ejercicio del data mart
# se cargan 6 meses (lo que recomienda el README del módulo): ene–jun 2024.
MONTHS_TO_INGEST = [
    (2024, 1), (2024, 2), (2024, 3),
    (2024, 4), (2024, 5), (2024, 6),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingesta incremental (append)
# MAGIC
# MAGIC Cada mes se lee con `read_yellow_trips_month`, que detecta
# MAGIC automáticamente la variante de esquema (v1/v2/v3) y devuelve
# MAGIC un DataFrame con el `UNIFIED_SCHEMA`.
# MAGIC
# MAGIC Antes de escribir, se verifica si el mes ya fue ingestado
# MAGIC consultando `_source_file` en la tabla bronze (`T_BRONZE_TRIPS`).
# MAGIC La primera ejecución crea la tabla; `spark.catalog.tableExists` lo detecta
# MAGIC sin tener que atrapar excepciones.

# COMMAND ----------

def _already_ingested(spark, year: int, month: int) -> bool:
    """Verifica si un mes ya fue ingestado en bronze."""
    # Si la tabla aún no existe, no hay nada ingestado
    if not spark.catalog.tableExists(T_BRONZE_TRIPS):
        return False

    pattern = f"%yellow_tripdata_{year}-{month:02d}%"
    return (
        spark.table(T_BRONZE_TRIPS)
        .filter(F.col("_source_file").like(pattern))
        .limit(1)
        .count()
    ) > 0

# COMMAND ----------

for year, month in MONTHS_TO_INGEST:
    print(f"\n{'─'*60}")
    print(f"  {year}-{month:02d}", end="")

    if _already_ingested(spark, year, month):
        print(f" → ⏭️  ya ingestado, saltando")
        continue

    print(f" → ingiriendo...")
    print(f"{'─'*60}")

    df = (
        read_yellow_trips_month(spark, year, month)
        .withColumn("_ingested_at", F.current_timestamp())
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(T_BRONZE_TRIPS)
    )

    print(f"  ✓ Append completado para {year}-{month:02d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación

# COMMAND ----------

T_BRONZE_TRIPS

# COMMAND ----------

bronze = spark.table(T_BRONZE_TRIPS)

bronze.agg(
    F.count("*").alias("total_rows"),
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
    F.countDistinct(
        F.year("tpep_pickup_datetime"),
        F.month("tpep_pickup_datetime")
    ).alias("distinct_months"),
).display()

# COMMAND ----------

# Archivos ya ingestados en bronze
(
    bronze
    .select(F.element_at(F.split("_source_file", "/"), -1).alias("archivo"))
    .distinct()
    .orderBy("archivo")
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Historial Delta
# MAGIC
# MAGIC Cada append queda registrado como una transacción independiente.
# MAGIC Esto permite time travel y auditoría de qué meses se cargaron.
# MAGIC
# MAGIC Como es una tabla de Unity Catalog, también puedes verla por SQL:
# MAGIC `DESCRIBE HISTORY <T_BRONZE_TRIPS>` o en Catalog Explorer → pestaña *History*.

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, T_BRONZE_TRIPS)
display(dt.history())
