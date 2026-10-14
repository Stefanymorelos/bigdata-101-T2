# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Store: Trip Duration Features
# MAGIC
# MAGIC Pipeline de feature engineering para predicción de duración de viaje.
# MAGIC
# MAGIC ## Diseño Feature Store
# MAGIC
# MAGIC | Aspecto | Valor |
# MAGIC |---------|-------|
# MAGIC | **Primary key** | `trip_id` (hash determinista) |
# MAGIC | **Timestamp key** | `pickup_ts` (event time para point-in-time lookups) |
# MAGIC | **Fuente** | Silver trips (capa limpia y enriquecida) |
# MAGIC | **Destino** | Tabla administrada UC → `T_GOLD_ML_FEATURES` (`gold_taxi_ml_features`) |
# MAGIC | **Modo** | Overwrite completo (batch offline features) |
# MAGIC
# MAGIC ## Features generadas
# MAGIC
# MAGIC | Feature | Tipo | Descripción |
# MAGIC |---------|------|-------------|
# MAGIC | `target_duration_min` | target | Duración real del viaje (label) |
# MAGIC | `pickup_hour` | temporal | Hora de recogida (0–23) |
# MAGIC | `pickup_dayofweek` | temporal | Día de la semana (1=Dom, 7=Sáb) |
# MAGIC | `is_weekend` | temporal | Fin de semana (0/1) |
# MAGIC | `is_rush_hour` | temporal | Hora pico (0/1) |
# MAGIC | `trip_distance` | espacial | Distancia del viaje en millas |
# MAGIC | `PULocationID` / `DOLocationID` | espacial | Zona TLC origen/destino |
# MAGIC | `pickup_borough` / `dropoff_borough` | espacial | Borough origen/destino |
# MAGIC | `passenger_count` | demanda | Número de pasajeros |
# MAGIC | `RatecodeID` | contexto | Tipo de tarifa |

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lectura de Silver

# COMMAND ----------

silver_df = spark.table(T_SILVER_TRIPS)

print(f"✓ Silver trips cargado: {T_SILVER_TRIPS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Filtros de elegibilidad
# MAGIC
# MAGIC Solo viajes que tienen sentido para entrenar un modelo de duración:
# MAGIC - Boroughs conocidos: sin nulls y sin las zonas especiales 264 (`Unknown`)
# MAGIC   y 265 (`Outside of NYC`), que silver conserva pero no aportan ubicación útil
# MAGIC - Sin nulls en `passenger_count` ni `RatecodeID` (excluye Flex Fare,
# MAGIC   `payment_type = 0`, que la TLC publica sin esas columnas)
# MAGIC - Duración entre 2 y 120 minutos (elimina cancelaciones y outliers extremos)
# MAGIC - Distancia entre 0.2 y 50 millas (elimina errores GPS y viajes interurbanos)

# COMMAND ----------

UNKNOWN_ZONE_IDS = [264, 265]

eligible_df = silver_df.filter(
    F.col("pickup_borough").isNotNull()
    & F.col("dropoff_borough").isNotNull()
    & ~F.col("PULocationID").isin(UNKNOWN_ZONE_IDS)
    & ~F.col("DOLocationID").isin(UNKNOWN_ZONE_IDS)
    # Flex Fare (payment_type = 0) llega sin passenger_count ni RatecodeID, que
    # son features del modelo: se excluyen aquí en vez de dejar nulls en la tabla
    & F.col("passenger_count").isNotNull()
    & F.col("RatecodeID").isNotNull()
    & F.col("trip_duration_min").between(2, 120)
    & F.col("trip_distance").between(0.2, 50)
)

total_silver = silver_df.count()
total_eligible = eligible_df.count()
pct_eligible = 100.0 * total_eligible / total_silver if total_silver > 0 else 0

print(f"  Silver total:    {total_silver:>12,}")
print(f"  Elegibles ML:    {total_eligible:>12,} ({pct_eligible:.1f}%)")
print(f"  Descartados:     {total_silver - total_eligible:>12,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Feature engineering
# MAGIC
# MAGIC Construcción del feature set con:
# MAGIC - **Primary key**: hash determinista sobre campos que identifican un viaje único
# MAGIC - **Timestamp key**: `pickup_ts` para point-in-time correctness en training/serving
# MAGIC - **Metadata**: `_created_at` para auditoría del pipeline

# COMMAND ----------

ml_features_df = (
    eligible_df
    .select(
        # --- Primary key (determinista, reproducible) ---
        F.sha2(
            F.concat_ws("||",
                F.col("VendorID").cast("string"),
                F.col("tpep_pickup_datetime").cast("string"),
                F.col("tpep_dropoff_datetime").cast("string"),
                F.col("PULocationID").cast("string"),
                F.col("DOLocationID").cast("string"),
            ), 256
        ).alias("trip_id"),

        # --- Timestamp key (event time) ---
        F.col("tpep_pickup_datetime").alias("pickup_ts"),

        # --- Target ---
        F.col("trip_duration_min").alias("target_duration_min"),

        # --- Features espaciales ---
        "PULocationID", "DOLocationID",
        "pickup_borough", "dropoff_borough",
        "trip_distance",

        # --- Features temporales ---
        "pickup_hour", "pickup_dayofweek",
        F.col("is_weekend").cast("int").alias("is_weekend"),
        F.col("is_rush_hour").cast("int").alias("is_rush_hour"),

        # --- Features de contexto ---
        "passenger_count",
        "RatecodeID",

        # --- Metadata ---
        "pickup_date",
    )
    .withColumn("_created_at", F.current_timestamp())
)

# COMMAND ----------

display(ml_features_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validación pre-escritura
# MAGIC
# MAGIC Checks básicos antes de persistir:

# COMMAND ----------

# Verificar que no hay nulls en primary key ni timestamp key
null_checks = ml_features_df.select(
    F.sum(F.when(F.col("trip_id").isNull(), 1).otherwise(0)).alias("null_trip_id"),
    F.sum(F.when(F.col("pickup_ts").isNull(), 1).otherwise(0)).alias("null_pickup_ts"),
    F.sum(F.when(F.col("target_duration_min").isNull(), 1).otherwise(0)).alias("null_target"),
    F.countDistinct("trip_id").alias("unique_keys"),
    F.count("*").alias("total_rows"),
).first()

print("─── Validación pre-escritura ───")
print(f"  Nulls en trip_id:    {null_checks['null_trip_id']}")
print(f"  Nulls en pickup_ts:  {null_checks['null_pickup_ts']}")
print(f"  Nulls en target:     {null_checks['null_target']}")
print(f"  Keys únicas:         {null_checks['unique_keys']:,}")
print(f"  Total rows:          {null_checks['total_rows']:,}")

dup_rate = 1 - (null_checks['unique_keys'] / null_checks['total_rows']) if null_checks['total_rows'] > 0 else 0
if dup_rate > 0.01:
    print(f"  ⚠️  Tasa de duplicados en PK: {dup_rate:.2%}")
else:
    print(f"  ✓ Tasa de duplicados en PK: {dup_rate:.4%} (aceptable)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Persistencia en Feature Store (Gold)
# MAGIC
# MAGIC Escritura como tabla administrada de Unity Catalog con overwrite completo.
# MAGIC En producción se declararía `trip_id` como primary key y se usaría
# MAGIC Databricks Feature Engineering (`fe.write_table()`), que requiere
# MAGIC precisamente una tabla de UC como esta.

# COMMAND ----------

(
    ml_features_df.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_GOLD_ML_FEATURES)
)

print(f"✓ Feature table escrita en {T_GOLD_ML_FEATURES}")
print(f"  Registros: {null_checks['total_rows']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verificación post-escritura

# COMMAND ----------

features_written = spark.table(T_GOLD_ML_FEATURES)

display(
    features_written.agg(
        F.count("*").alias("rows"),
        F.countDistinct("trip_id").alias("unique_trips"),
        F.round(F.avg("target_duration_min"), 2).alias("avg_duration_min"),
        F.round(F.stddev("target_duration_min"), 2).alias("std_duration_min"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
        F.min("pickup_ts").alias("min_pickup"),
        F.max("pickup_ts").alias("max_pickup"),
        F.min("_created_at").alias("pipeline_ts"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Distribución de features (sanity check)

# COMMAND ----------

display(
    features_written
    .groupBy("pickup_borough")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("target_duration_min"), 1).alias("avg_duration"),
        F.round(F.avg("trip_distance"), 1).alias("avg_distance"),
    )
    .orderBy(F.desc("trips"))
)
