# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: limpieza, validación y enriquecimiento (incremental)
# MAGIC
# MAGIC Este notebook implementa la capa Silver del patrón Medallion con procesamiento **incremental**:
# MAGIC
# MAGIC 1. **Validación de schema** — Verificar que bronze no cambió de estructura
# MAGIC 2. **Detección de nuevos datos** — Identificar meses en bronze que aún no están en Silver
# MAGIC 3. **Exploración** — Analizar los datos nuevos para identificar problemas de calidad
# MAGIC 4. **Deduplicación** — Eliminar registros duplicados por re-ingestas
# MAGIC 5. **Umbrales dinámicos** — Calcular percentiles (p99) para definir límites de outliers
# MAGIC 6. **Reglas de calidad** — Filtrar registros inválidos
# MAGIC 7. **Cuarentena** — Registros rechazados con motivos explícitos (`rejection_reasons`)
# MAGIC 8. **Enriquecimiento** — Broadcast join con zonas + features temporales y económicas
# MAGIC 9. **Persistencia** — `replaceWhere` incremental particionado por año/mes
# MAGIC 10. **Métricas de calidad** — Tabla histórica para monitoreo de SLA
# MAGIC
# MAGIC Los hallazgos y reglas se documentan a lo largo del notebook conforme se descubren.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce

# COMMAND ----------

bronze = spark.table(T_BRONZE_TRIPS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validación de schema

# COMMAND ----------

EXPECTED_COLUMNS = {field.name for field in UNIFIED_SCHEMA.fields} | {"_source_file", "_ingested_at"}
actual_columns = set(bronze.columns)
missing = EXPECTED_COLUMNS - actual_columns
unexpected = actual_columns - EXPECTED_COLUMNS

if missing:
    print(f"⚠️  Columnas FALTANTES en bronze: {missing}")
if unexpected:
    print(f"ℹ️  Columnas NUEVAS en bronze (no esperadas): {unexpected}")
if not missing:
    print("✓ Schema de bronze validado correctamente")

assert not missing, f"Faltan columnas en bronze: {missing}. Revisa la ingesta."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Detección de datos nuevos (incremental)
# MAGIC
# MAGIC Para reprocesar meses anteriores, usar el widget `force_reprocess`:
# MAGIC - `"all"` → reprocesar todo bronze
# MAGIC - `"2024-01,2024-02,2025-03"` → reprocesar esos meses (formato YYYY-MM)
# MAGIC - *(vacío)* → modo incremental normal (solo archivos nuevos)

# COMMAND ----------

dbutils.widgets.text("force_reprocess", "", "Reprocesar: vacío=incremental, all=todo, o YYYY-MM separados por coma")
FORCE_REPROCESS = dbutils.widgets.get("force_reprocess").strip()

# El widget conserva el último valor escrito aunque vuelvas a correr esta celda:
# dbutils.widgets.text() solo pone el default la primera vez que crea el widget.
# Se imprime para que se vea con qué valor corre realmente el notebook.
print(f"force_reprocess = {FORCE_REPROCESS!r}")

# COMMAND ----------

# Expresión reutilizable para extraer nombre de archivo
_file_name_expr = F.element_at(F.split("_source_file", "/"), -1)

# Archivos disponibles en bronze
bronze_files = bronze.select(_file_name_expr.alias("_file_name")).distinct()
all_bronze_files = [row["_file_name"] for row in bronze_files.collect()]

# Determinar qué archivos procesar
silver_exists = False

if FORCE_REPROCESS.lower() == "all":
    new_file_names = all_bronze_files
    print(f"⚠️  MODO REPROCESAMIENTO TOTAL: {len(new_file_names)} archivos")

elif FORCE_REPROCESS:
    months_requested = [m.strip() for m in FORCE_REPROCESS.split(",") if m.strip()]
    new_file_names = [f for f in all_bronze_files if any(m in f for m in months_requested)]
    not_found = [m for m in months_requested if not any(m in f for f in all_bronze_files)]
    if not_found:
        print(f"⚠️  Meses no encontrados en bronze: {not_found}")
    print(f"⚠️  MODO REPROCESAMIENTO SELECTIVO: {len(new_file_names)} archivos para {months_requested}")

elif spark.catalog.tableExists(T_SILVER_TRIPS):
    silver_files = (
        spark.table(T_SILVER_TRIPS)
        .select(_file_name_expr.alias("_file_name"))
        .distinct()
    )
    new_file_names = [
        row["_file_name"]
        for row in bronze_files.subtract(silver_files).collect()
    ]
    silver_exists = True

else:
    # Primera ejecución: Silver aún no existe, se procesa todo bronze
    new_file_names = all_bronze_files

if not new_file_names:
    print("✓ No hay datos nuevos por procesar. Silver está al día.")
    dbutils.notebook.exit("NO_NEW_DATA")

print(f"✓ Archivos a procesar: {len(new_file_names)}")
for f in sorted(new_file_names):
    print(f"    • {f}")

# Verificar si Silver ya existe (para replaceWhere vs creación)
if FORCE_REPROCESS:
    silver_exists = spark.catalog.tableExists(T_SILVER_TRIPS)

# COMMAND ----------

# Filtrar bronze solo a los archivos del lote
bronze_new = bronze.filter(_file_name_expr.isin(new_file_names))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Exploración de datos nuevos

# COMMAND ----------

bronze_new.printSchema()

# COMMAND ----------

bronze_new.agg(
    F.count("*").alias("total_counts"),
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
).display()

# COMMAND ----------

bronze_new.select(
    "passenger_count", "trip_distance", "fare_amount", "tip_amount", "total_amount"
).describe().display()

# COMMAND ----------

bronze_new.select(
    F.sum(F.when(F.col("trip_distance") <= 0, 1).otherwise(0)).alias("invalid_trip_distance"),
    F.sum(F.when(F.col("fare_amount") < 0, 1).otherwise(0)).alias("negative_fare"),
    F.sum(F.when(F.col("passenger_count") == 0, 1).otherwise(0)).alias("zero_passenger"),
    F.sum(F.when(F.col("passenger_count").isNull(), 1).otherwise(0)).alias("null_passengers"),
    F.sum(F.when(F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"), 1).otherwise(0)).alias("invalid_dropoff"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deduplicación

# COMMAND ----------

DEDUP_KEYS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"]
dedup_window = Window.partitionBy(*DEDUP_KEYS).orderBy(F.col("_ingested_at").desc())

bronze_deduped = (
    bronze_new
    .withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

# Contar ambos en un solo paso (bronze_new se escanea una vez por el window)
count_bronze_new = bronze_new.count()
count_deduped = bronze_deduped.count()
dupes_removed = count_bronze_new - count_deduped

print(f"✓ Registros en lote:     {count_bronze_new:,}")
print(f"✓ Duplicados eliminados: {dupes_removed:,}")
print(f"✓ Registros tras dedup:  {count_deduped:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Columnas derivadas y umbrales

# COMMAND ----------

file_year_month = F.regexp_extract(_file_name_expr, r"(\d{4}-\d{2})", 1)

trips = (
    bronze_deduped
    .withColumn("trip_duration_min",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
    .withColumn("_file_year_month", file_year_month)
    .withColumn("_pickup_year_month", F.date_format("tpep_pickup_datetime", "yyyy-MM"))
    .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
    .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
)

# COMMAND ----------

# Umbrales dinámicos: percentil 99 sobre todo bronze (estabilidad entre ejecuciones)
_pctiles = (
    bronze
    .filter((F.col("trip_distance") > 0) & (F.col("fare_amount") >= 0)
            & (F.col("total_amount") > 0) & (F.col("tip_amount") >= 0))
    .select(
        F.percentile_approx("trip_distance", 0.99).alias("distance_p99"),
        F.percentile_approx("fare_amount", 0.99).alias("fare_p99"),
        F.percentile_approx("total_amount", 0.99).alias("total_p99"),
        F.percentile_approx("tip_amount", 0.99).alias("tip_p99"),
    )
    .first()
)

MAX_TRIP_DISTANCE_MI = float(_pctiles["distance_p99"])
MAX_FARE_AMOUNT      = float(_pctiles["fare_p99"])
MAX_TOTAL_AMOUNT     = float(_pctiles["total_p99"])
MAX_TIP_AMOUNT       = float(_pctiles["tip_p99"])
MAX_SPEED_MPH        = 70.0
# 265 = todas las zonas del lookup de la TLC. Las dos últimas son especiales:
# 264 = "Unknown" y 265 = "Outside of NYC". Son viajes reales con ubicación no
# asignable, así que se conservan; gold decide si los excluye para su análisis.
NUM_TLC_ZONES        = 265
VALID_VENDOR_IDS     = [1, 2]
VALID_RATE_CODES     = [1, 2, 3, 4, 5, 6]
# 0 = "Flex Fare trip" según el diccionario de datos de la TLC. Es el valor que
# más crece desde 2025 (~24% de los viajes), así que excluirlo sesga los KPIs.
VALID_PAYMENT_TYPES  = [0, 1, 2, 3, 4, 5, 6]

print("─── Umbrales (p99 sobre todo bronze) ───")
print(f"  distance = {MAX_TRIP_DISTANCE_MI:.2f} mi")
print(f"  fare     = ${MAX_FARE_AMOUNT:.2f}")
print(f"  total    = ${MAX_TOTAL_AMOUNT:.2f}")
print(f"  tip      = ${MAX_TIP_AMOUNT:.2f}")
print(f"  speed    = {MAX_SPEED_MPH:.0f} mph")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Reglas de calidad

# COMMAND ----------

rules = {
    "valid_pickup_month":  F.col("_pickup_year_month") == F.col("_file_year_month"),
    "valid_dropoff":       F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"),
    "valid_duration":      F.col("trip_duration_min").between(1, 360),
    "valid_distance":      (F.col("trip_distance") > 0) & (F.col("trip_distance") <= MAX_TRIP_DISTANCE_MI),
    "valid_speed":         F.coalesce(F.try_divide(F.col("trip_distance"), F.col("trip_duration_min") / 60.0), F.lit(0.0)) <= MAX_SPEED_MPH,
    "valid_passengers":    F.col("passenger_count").between(1, 8),
    "valid_fare":          (F.col("fare_amount") >= 0) & (F.col("fare_amount") <= MAX_FARE_AMOUNT),
    "valid_tip":           (F.col("tip_amount") >= 0) & (F.col("tip_amount") <= MAX_TIP_AMOUNT),
    "valid_total":         (F.col("total_amount") > 0) & (F.col("total_amount") <= MAX_TOTAL_AMOUNT),
    "valid_fare_vs_total": F.col("fare_amount") <= F.col("total_amount"),
    "valid_pu_location":   F.col("PULocationID").between(1, NUM_TLC_ZONES),
    "valid_do_location":   F.col("DOLocationID").between(1, NUM_TLC_ZONES),
    "valid_vendor":        F.col("VendorID").isin(VALID_VENDOR_IDS),
    "valid_rate_code":     F.col("RatecodeID").isin(VALID_RATE_CODES),
    "valid_payment_type":  F.col("payment_type").isin(VALID_PAYMENT_TYPES),
}

# Flex Fare (payment_type = 0): la TLC publica estos viajes SIN passenger_count,
# RatecodeID, store_and_fwd_flag, congestion_surcharge ni airport_fee (todos null).
# Para ellos, null en pasajeros y tarifa es lo esperado, no un error.
_is_flex_fare = F.col("payment_type") == 0
rules["valid_passengers"] = (_is_flex_fare & F.col("passenger_count").isNull()) | rules["valid_passengers"]
rules["valid_rate_code"]  = (_is_flex_fare & F.col("RatecodeID").isNull()) | rules["valid_rate_code"]

# Reglas null-safe: en Spark, `null BETWEEN 1 AND 8` es null, no false. Sin esto,
# una fila con un null en cualquier regla queda con _is_valid = null y desaparece
# en silencio: filter(_is_valid) y filter(~_is_valid) la descartan a la vez, y
# tampoco aparece en rejection_reasons. Con coalesce, null cuenta como regla fallida.
rules = {name: F.coalesce(expr, F.lit(False)) for name, expr in rules.items()}

is_valid = reduce(lambda a, b: a & b, rules.values())

# COMMAND ----------

print(is_valid)  # expresión combinada de todas las reglas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Etiquetar, separar y construir motivos de rechazo

# COMMAND ----------

# Construir todas las columnas en un solo .select() para optimizar el plan lógico
rejection_reasons_expr = F.array_compact(
    F.array(*[F.when(~expr, F.lit(name)) for name, expr in rules.items()])
)

tagged = trips.select(
    "*",
    *[expr.alias(name) for name, expr in rules.items()],
    rejection_reasons_expr.alias("rejection_reasons"),
    is_valid.alias("_is_valid"),
)

# Timestamp de procesamiento Silver
silver_ts = F.current_timestamp()
rule_names = list(rules.keys())
_drop_valid = rule_names + ["_file_year_month", "_pickup_year_month", "rejection_reasons", "_is_valid"]

valid_df = (
    tagged.filter(F.col("_is_valid"))
    .drop(*_drop_valid)
    .withColumn("_ingested_at", silver_ts)
)

rejected_df = (
    tagged.filter(~F.col("_is_valid"))
    .drop("_file_year_month", "_pickup_year_month", "_is_valid")
    .withColumn("_ingested_at", silver_ts)
    .withColumn("rejected_at", silver_ts)
)

# Contar usando el tagged cacheado implícitamente por Spark
counts = {r["_is_valid"]: r["count"] for r in tagged.groupBy("_is_valid").count().collect()}
count_valid = counts.get(True, 0)
count_rejected = counts.get(False, 0)

# Cuadre: toda fila del lote es válida o rechazada; ninguna puede perderse.
assert None not in counts, f"{counts[None]:,} filas con _is_valid = null: alguna regla no es null-safe"
assert count_valid + count_rejected == count_deduped, (
    f"Descuadre: {count_valid:,} válidos + {count_rejected:,} rechazados != {count_deduped:,} tras dedup"
)

print(f"✓ Válidos:    {count_valid:>12,}")
print(f"✓ Rechazados: {count_rejected:>12,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Enriquecimiento: zonas y features temporales/económicas

# COMMAND ----------

# --- Broadcast join con zonas ---
zones = spark.table(T_BRONZE_ZONES)

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

valid_df = (
    valid_df
    .join(F.broadcast(zones_pickup),  on="PULocationID", how="left")
    .join(F.broadcast(zones_dropoff), on="DOLocationID", how="left")
)

# COMMAND ----------

# --- Features temporales y económicas ---
valid_df = (
    valid_df
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

# MAGIC %md
# MAGIC ## 9. Persistir en Silver (`replaceWhere` por partición)
# MAGIC
# MAGIC ### Estrategia elegida: `replaceWhere`
# MAGIC
# MAGIC | Criterio | replaceWhere | MERGE |
# MAGIC |----------|-------------|-------|
# MAGIC | Semántica | Reemplaza particiones completas | Compara registro por registro |
# MAGIC | Performance | Solo reescribe particiones afectadas | JOIN entre source y target |
# MAGIC | Consistencia | Si un registro cambia de válido→rechazado, desaparece | MERGE no borra lo que ya no está en source |
# MAGIC | Caso ideal | Procesar meses completos (nuestro caso) | Actualizaciones puntuales |
# MAGIC
# MAGIC ### Alternativa: MERGE con partition pruning
# MAGIC
# MAGIC ```python
# MAGIC # Ejemplo (NO usado aquí):
# MAGIC from delta.tables import DeltaTable
# MAGIC MERGE_KEYS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
# MAGIC               "PULocationID", "DOLocationID"]
# MAGIC cond = " AND ".join([f"t.{k} = s.{k}" for k in MERGE_KEYS])
# MAGIC cond += " AND t.pickup_year = s.pickup_year AND t.pickup_month = s.pickup_month"
# MAGIC DeltaTable.forName(spark, T_SILVER_TRIPS).alias("t") \
# MAGIC     .merge(source_df.alias("s"), cond) \
# MAGIC     .whenMatchedUpdateAll() \
# MAGIC     .whenNotMatchedInsertAll() \
# MAGIC     .execute()
# MAGIC ```
# MAGIC
# MAGIC Ref: [Databricks KB - MERGE partition pruning](https://kb.databricks.com/delta/delta-merge-into)

# COMMAND ----------

# Particiones presentes en el lote
partitions_in_batch = (
    valid_df.select("pickup_year", "pickup_month")
    .union(rejected_df.select("pickup_year", "pickup_month"))
    .distinct()
    .collect()
)

replace_where_expr = " OR ".join([
    f"(pickup_year = {r['pickup_year']} AND pickup_month = {r['pickup_month']})"
    for r in partitions_in_batch
])

print("─── Particiones a reemplazar ───")
for r in sorted(partitions_in_batch, key=lambda x: (x["pickup_year"], x["pickup_month"])):
    print(f"  • {r['pickup_year']}-{r['pickup_month']:02d}")

# COMMAND ----------

def _write_partitioned(df, table_name: str, exists: bool) -> None:
    """Reemplaza las particiones del lote si la tabla existe; si no, la crea particionada."""
    writer = df.write.format("delta")
    if exists:
        writer.mode("overwrite").option("replaceWhere", replace_where_expr).saveAsTable(table_name)
    else:
        writer.partitionBy("pickup_year", "pickup_month").saveAsTable(table_name)

# COMMAND ----------

# --- Silver válidos ---
_write_partitioned(valid_df, T_SILVER_TRIPS, silver_exists)
print(f"✓ Silver trips → {T_SILVER_TRIPS}")

# COMMAND ----------

# --- Silver rechazados ---
_write_partitioned(rejected_df, T_SILVER_REJECTED, spark.catalog.tableExists(T_SILVER_REJECTED))
print(f"✓ Silver rejected → {T_SILVER_REJECTED}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Métricas de calidad (tabla histórica)

# COMMAND ----------

# Failures por regla en una sola pasada
rule_failures = tagged.select(
    *[F.sum(F.when(~F.col(name), 1).otherwise(0)).alias(name) for name in rule_names]
).first()

from pyspark.sql import Row
from datetime import datetime

total_lote = count_valid + count_rejected

metrics_df = spark.createDataFrame([Row(
    execution_ts=datetime.now(),
    files_processed=new_file_names,
    bronze_lote=total_lote + dupes_removed,
    duplicates_removed=dupes_removed,
    silver_valid=count_valid,
    silver_rejected=count_rejected,
    pct_valid=round(100.0 * count_valid / total_lote, 2) if total_lote > 0 else 0.0,
    pct_rejected=round(100.0 * count_rejected / total_lote, 2) if total_lote > 0 else 0.0,
    thresholds={"distance": MAX_TRIP_DISTANCE_MI, "fare": MAX_FARE_AMOUNT,
                "total": MAX_TOTAL_AMOUNT, "tip": MAX_TIP_AMOUNT, "speed": MAX_SPEED_MPH},
    rule_failures={name: int(rule_failures[name]) for name in rule_names},
)])

metrics_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(T_SILVER_QUALITY_METRICS)
print(f"✓ Métricas → {T_SILVER_QUALITY_METRICS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de ejecución

# COMMAND ----------

total_silver = spark.table(T_SILVER_TRIPS).count()
total_rejected_all = spark.table(T_SILVER_REJECTED).count()

print("═" * 60)
print(f"  LOTE: {len(new_file_names)} archivos")
print(f"  Bronze:     {count_bronze_new:>12,}")
print(f"  Duplicados: {dupes_removed:>12,}")
print(f"  → Válidos:    {count_valid:>12,}  ({100*count_valid/total_lote:.1f}%)")
print(f"  → Rechazados: {count_rejected:>12,}  ({100*count_rejected/total_lote:.1f}%)")
print("─" * 60)
print(f"  ACUMULADO SILVER")
print(f"  Válidos:    {total_silver:>12,}")
print(f"  Rechazados: {total_rejected_all:>12,}")
print("═" * 60)
print("\n─── Rechazos por regla ───")
for name in rule_names:
    n = int(rule_failures[name])
    print(f"  {name:<25} {n:>10,}  ({100*n/total_lote:.1f}%)")