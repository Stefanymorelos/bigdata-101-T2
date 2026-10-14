# Databricks notebook source
# MAGIC %md
# MAGIC # Data mart Finanzas TLC — 02. Tabla de hechos `fact_trips`
# MAGIC
# MAGIC **Parte 2** del ejercicio. También aplica el **diseño físico** que se justifica en la Parte 5.3.
# MAGIC
# MAGIC - **Grano:** un viaje. La tabla tiene exactamente las mismas filas que `T_SILVER_TRIPS_ENRICHED`.
# MAGIC - **Llaves foráneas:** `pickup_date_key`, `pickup_hour_key`, `pickup_zone_key`, `dropoff_zone_key`,
# MAGIC   `payment_type_key`, `rate_code_key`, `vendor_key`.
# MAGIC - **Medidas:** `passenger_count`, `trip_distance`, `trip_duration_min`, los 9 componentes del cobro
# MAGIC   y `total_amount`.
# MAGIC - **Trazabilidad:** `tpep_pickup_datetime` y `tpep_dropoff_datetime`. Junto con `vendor_key` y las dos
# MAGIC   llaves de zona forman la llave natural con la que silver deduplica.
# MAGIC - **Sin atributos descriptivos.** Borough, nombre de zona y descripción de pago viven en las dimensiones.
# MAGIC
# MAGIC Requisito: haber corrido `01_dm_dimensiones`.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ./_dm_common

# COMMAND ----------

silver = spark.table(T_SILVER_TRIPS_ENRICHED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Decisión sobre nulos en las medidas
# MAGIC
# MAGIC | Medida | Cuándo llega nula | Decisión | Por qué |
# MAGIC |---|---|---|---|
# MAGIC | `passenger_count` | Flex Fare | **`NULL`** | No es que viajaran 0 pasajeros: el dato **no se conoce**. Con 0, `AVG(passenger_count)` bajaría artificialmente |
# MAGIC | `congestion_surcharge` | Flex Fare | **`NULL`** | La TLC **no publica** el recargo de esos viajes, pero eso no significa que no se haya cobrado. Es un valor desconocido, no un cero |
# MAGIC | `airport_fee` | Flex Fare | **`NULL`** | Mismo caso: desconocido |
# MAGIC | `cbd_congestion_fee` | Viajes antes de 2025 | **`0`** | El cargo **no existía** (empezó el 5 de enero de 2025), así que el valor sí se conoce: se cobraron $0. Es un cero estructural, no un dato faltante |
# MAGIC
# MAGIC La regla general es: **`NULL` = "no sé"** y **`0` = "sé que fue cero"**.
# MAGIC
# MAGIC ### ¿Cómo cambia `AVG(congestion_surcharge)` con cada opción?
# MAGIC
# MAGIC - **Con `NULL`:** `AVG` ignora los nulos, así que el promedio es `SUM / COUNT(viajes con dato)`. Da el recargo
# MAGIC   promedio **de los viajes en que se conoce**.
# MAGIC - **Con `0`:** los viajes Flex Fare entran en el denominador con 0. Si el 24 % de los viajes de 2025 es Flex Fare
# MAGIC   (ver el comentario de `VALID_PAYMENT_TYPES` en silver), el promedio cae cerca de un 24 %. Por ejemplo, si el
# MAGIC   promedio real fuera $2,50, se reportaría $2,50 × 0,76 ≈ $1,90. Finanzas vería una **caída ficticia** del recargo
# MAGIC   justo cuando crece Flex Fare.
# MAGIC - **`SUM` es igual en los dos casos.** El revenue total no cambia, solo cambian los promedios y los conteos.
# MAGIC
# MAGIC Para `cbd_congestion_fee` aplica el razonamiento contrario. Antes de 2025 el promedio verdadero **es** $0,
# MAGIC y así `AVG` sobre 2023–2026 da el cargo real por viaje del periodo. Además, una suma por fila como
# MAGIC `congestion_surcharge + airport_fee + cbd_congestion_fee` no se vuelve `NULL` en todos los viajes de 2023–2024.
# MAGIC
# MAGIC **Consecuencia para quien consulta:** las sumas por fila de medidas que pueden ser `NULL` se hacen con
# MAGIC `COALESCE` o sumando cada columna por separado (`SUM(a) + SUM(b)`), como en la pregunta 6 de la Parte 4.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Construcción
# MAGIC
# MAGIC 1. Se toman de silver los códigos, con cast a `INT`, igual que las llaves de las dimensiones (silver los trae como `LONG` o `DOUBLE`).
# MAGIC 2. Cada código se resuelve contra su dimensión con **`left join` + `broadcast`**, y la llave queda en
# MAGIC    `coalesce(llave_dim, -1)`:
# MAGIC    - un código nulo (`RatecodeID` en Flex Fare) → `-1`;
# MAGIC    - un código que no existe en el catálogo → `-1` (en vez de una llave huérfana).
# MAGIC
# MAGIC    Un `inner join` descartaría esos viajes **en silencio** y la conciliación fallaría.
# MAGIC 3. Las llaves de fecha y hora no llevan `-1`: silver garantiza `tpep_pickup_datetime`, y `dim_date` y `dim_time`
# MAGIC    cubren todo el rango. Si algo fallara, la conciliación lo detecta como llave huérfana.

# COMMAND ----------

base = silver.select(
    # Llaves de tiempo (calculadas igual que en las dimensiones)
    F.date_format("pickup_date", "yyyyMMdd").cast("int").alias("pickup_date_key"),
    F.col("pickup_hour").cast("int").alias("pickup_hour_key"),
    # Códigos de negocio → INT
    F.col("PULocationID").cast("int").alias("_pu_code"),
    F.col("DOLocationID").cast("int").alias("_do_code"),
    F.col("payment_type").cast("int").alias("_payment_code"),
    F.col("RatecodeID").cast("int").alias("_rate_code"),
    F.col("VendorID").cast("int").alias("_vendor_code"),
    # Trazabilidad
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    # Medidas
    F.col("passenger_count").cast("int").alias("passenger_count"),   # NULL en Flex Fare (se conserva)
    "trip_distance",
    "trip_duration_min",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",                                           # NULL en Flex Fare (se conserva)
    "airport_fee",                                                    # NULL en Flex Fare (se conserva)
    F.coalesce("cbd_congestion_fee", F.lit(0.0)).alias("cbd_congestion_fee"),  # antes de 2025 = 0
    "total_amount",
)


def resolve_key(df, code_col: str, dim_table: str, dim_key: str, fk_name: str):
    """left join (broadcast) contra la dimensión; llave = coalesce(llave_dim, -1)."""
    lk = f"_lk_{fk_name}"
    keys = spark.table(dim_table).select(F.col(dim_key).alias(lk)).filter(F.col(lk) != UNKNOWN_KEY)
    return (
        df.join(F.broadcast(keys), F.col(code_col) == F.col(lk), "left")
          .withColumn(fk_name, F.coalesce(F.col(lk), F.lit(UNKNOWN_KEY)))
          .drop(lk)
    )


fact = base
fact = resolve_key(fact, "_pu_code",      T_DM_DIM_ZONE,         "zone_key",         "pickup_zone_key")   # role-playing
fact = resolve_key(fact, "_do_code",      T_DM_DIM_ZONE,         "zone_key",         "dropoff_zone_key")  # role-playing
fact = resolve_key(fact, "_payment_code", T_DM_DIM_PAYMENT_TYPE, "payment_type_key", "payment_type_key")
fact = resolve_key(fact, "_rate_code",    T_DM_DIM_RATE_CODE,    "rate_code_key",    "rate_code_key")
fact = resolve_key(fact, "_vendor_code",  T_DM_DIM_VENDOR,       "vendor_key",       "vendor_key")

FACT_COLUMNS = [
    "pickup_date_key", "pickup_hour_key", "pickup_zone_key", "dropoff_zone_key",
    "payment_type_key", "rate_code_key", "vendor_key",
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "trip_duration_min",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "total_amount",
]
fact = fact.select(*FACT_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Escritura y diseño físico (Parte 5.3)
# MAGIC
# MAGIC **Decisión: liquid clustering con `CLUSTER BY (pickup_date_key, pickup_zone_key)`**, sin particionado ni `ZORDER`.
# MAGIC
# MAGIC Los filtros que más usan las preguntas de la Parte 4 son:
# MAGIC
# MAGIC | Filtro o agrupación | Preguntas |
# MAGIC |---|---|
# MAGIC | Fecha (mes, año, festivo, fin de semana) → `pickup_date_key` | 1, 2, 4, 5 y casi cualquier reporte financiero ("el mes pasado", "el trimestre") |
# MAGIC | Zona o borough de pickup → `pickup_zone_key` | 1, 6 |
# MAGIC | Zona de destino, forma de pago, tarifa | 2, 3, 4, 5 (columnas de baja cardinalidad o filtros ocasionales) |
# MAGIC
# MAGIC Por qué liquid clustering y no las alternativas:
# MAGIC
# MAGIC - **Particionar por fecha** generaría más de 1.000 particiones diarias de pocos MB cada una (problema de
# MAGIC   *small files*). Particionar por mes o año no ayuda al segundo filtro (zona), y la llave de partición no
# MAGIC   se puede cambiar sin reescribir toda la tabla.
# MAGIC - **`ZORDER`** hay que re-ejecutarlo completo en cada `OPTIMIZE` y no es incremental.
# MAGIC - **Liquid clustering** agrupa los archivos por las dos llaves, y el *data skipping* poda archivos tanto por
# MAGIC   rango de fechas como por zona. Es incremental (`OPTIMIZE` solo reorganiza lo nuevo) y las llaves se pueden
# MAGIC   cambiar después con `ALTER TABLE … CLUSTER BY` sin reescribir. No se combina con particionado ni con `ZORDER`,
# MAGIC   por eso no se usa ninguno de los dos.
# MAGIC
# MAGIC Las llaves de los filtros por borough o aeropuerto no van en el clustering. Esos filtros se resuelven en
# MAGIC `dim_zone` (265 filas, broadcast) y se convierten en una lista de `pickup_zone_key`, que es por donde está
# MAGIC agrupada la tabla.

# COMMAND ----------

# Versión Delta antes de escribir: si la conciliación falla, se restaura (ver 2.4)
PREV_VERSION = (
    spark.sql(f"DESCRIBE HISTORY {T_DM_FACT_TRIPS} LIMIT 1").first().version
    if spark.catalog.tableExists(T_DM_FACT_TRIPS) else None
)
print(f"Versión previa de fact_trips: {PREV_VERSION}")

write_mart_table(
    fact, T_DM_FACT_TRIPS,
    ddl="""
        pickup_date_key        INT       NOT NULL COMMENT 'FK → dim_date.date_key',
        pickup_hour_key        INT       NOT NULL COMMENT 'FK → dim_time.hour_key',
        pickup_zone_key        INT       NOT NULL COMMENT 'FK → dim_zone.zone_key (rol pickup)',
        dropoff_zone_key       INT       NOT NULL COMMENT 'FK → dim_zone.zone_key (rol dropoff)',
        payment_type_key       INT       NOT NULL COMMENT 'FK → dim_payment_type.payment_type_key',
        rate_code_key          INT       NOT NULL COMMENT 'FK → dim_rate_code.rate_code_key (-1 en Flex Fare)',
        vendor_key             INT       NOT NULL COMMENT 'FK → dim_vendor.vendor_key',
        tpep_pickup_datetime   TIMESTAMP_NTZ,
        tpep_dropoff_datetime  TIMESTAMP_NTZ,
        passenger_count        INT       COMMENT 'NULL en Flex Fare (desconocido)',
        trip_distance          DOUBLE    COMMENT 'Millas',
        trip_duration_min      DOUBLE    COMMENT 'Minutos',
        fare_amount            DOUBLE,
        extra                  DOUBLE,
        mta_tax                DOUBLE,
        tip_amount             DOUBLE    COMMENT 'Solo se registra con tarjeta: filtrar por dim_payment_type.tip_is_recorded',
        tolls_amount           DOUBLE,
        improvement_surcharge  DOUBLE,
        congestion_surcharge   DOUBLE    COMMENT 'NULL en Flex Fare (desconocido)',
        airport_fee            DOUBLE    COMMENT 'NULL en Flex Fare (desconocido)',
        cbd_congestion_fee     DOUBLE    COMMENT '0 antes de 2025 (el cargo no existía)',
        total_amount           DOUBLE
    """,
    comment="Hechos de viajes de taxi amarillo. Grano: un viaje (= una fila de silver_yellow_trips_enriched)",
    cluster_by="pickup_date_key, pickup_zone_key",
)

# Agrupa físicamente los archivos según las llaves de clustering
spark.sql(f"OPTIMIZE {T_DM_FACT_TRIPS}")
print(f"✓ OPTIMIZE {T_DM_FACT_TRIPS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación rápida

# COMMAND ----------

f = spark.table(T_DM_FACT_TRIPS)
display(f.limit(10))

display(spark.sql(f"""
    SELECT p.payment_type_desc,
           COUNT(*)                                      AS viajes,
           COUNT_IF(f.rate_code_key = -1)                AS sin_tarifa,
           COUNT_IF(f.passenger_count IS NULL)           AS sin_pasajeros,
           COUNT_IF(f.congestion_surcharge IS NULL)      AS sin_congestion
    FROM {T_DM_FACT_TRIPS} f
    JOIN {T_DM_DIM_PAYMENT_TYPE} p ON f.payment_type_key = p.payment_type_key
    GROUP BY p.payment_type_desc
    ORDER BY viajes DESC
"""))

display(spark.sql(f"DESCRIBE DETAIL {T_DM_FACT_TRIPS}").select("clusteringColumns", "numFiles", "sizeInBytes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Conciliación (obligatoria)
# MAGIC
# MAGIC Si **cualquiera** de estos controles falla, la celda lanza `AssertionError` y la tarea del workflow queda en error.
# MAGIC
# MAGIC 1. `COUNT(*)` de la tabla de hechos = `COUNT(*)` de silver.
# MAGIC 2. `SUM(total_amount)` de la tabla de hechos = `SUM(total_amount)` de silver. Se compara la suma de
# MAGIC    `CAST(total_amount AS DECIMAL(38,2))` para que sea **exacta**. Sumar `DOUBLE` en paralelo puede dar
# MAGIC    diferencias en el último decimal según el orden en que Spark suma las particiones. Eso daría falsos
# MAGIC    positivos, o habría que usar una tolerancia que podría esconder errores reales.
# MAGIC 3. Cero llaves huérfanas: un `left_anti` de cada FK contra su dimensión.
# MAGIC 4. Cero llaves duplicadas o nulas en cada dimensión.
# MAGIC
# MAGIC Se ejecutan **todos** los controles y al final se lanza un solo error con la lista completa de fallas.
# MAGIC Así quien atienda la alerta ve todo de una vez.
# MAGIC
# MAGIC **Restauración automática.** Fallar el job evita que corran las tareas siguientes, pero las vistas de Finanzas
# MAGIC leen `fact_trips` **en vivo**. Si la tabla mala se quedara escrita, la verían igual. Por eso, antes de fallar, la
# MAGIC celda hace `RESTORE TABLE … TO VERSION AS OF <versión previa>` con time travel de Delta, y Finanzas sigue viendo
# MAGIC los datos del día anterior. La alternativa más estricta es el patrón *write-audit-publish*: escribir en una tabla de
# MAGIC staging, conciliar ahí y solo entonces publicar. Ese patrón evita incluso los minutos en que la tabla mala está
# MAGIC visible, a cambio de escribir los datos dos veces.

# COMMAND ----------

def _sum_exact(table):
    return spark.sql(f"SELECT SUM(CAST(total_amount AS DECIMAL(38,2))) AS s FROM {table}").first().s

errors = []
checks = []

# 1. Conteo
n_silver = spark.table(T_SILVER_TRIPS_ENRICHED).count()
n_fact = spark.table(T_DM_FACT_TRIPS).count()
checks.append(("1. COUNT(*)", f"silver={n_silver:,}  fact={n_fact:,}", n_silver == n_fact))

# 2. Suma de total_amount
s_silver, s_fact = _sum_exact(T_SILVER_TRIPS_ENRICHED), _sum_exact(T_DM_FACT_TRIPS)
checks.append(("2. SUM(total_amount)", f"silver={s_silver:,}  fact={s_fact:,}", s_silver == s_fact))

# 3. Llaves huérfanas
for fk, (dim_table, dim_key) in FACT_FKS.items():
    n_orphans = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {T_DM_FACT_TRIPS} f
        LEFT ANTI JOIN {dim_table} d ON f.{fk} = d.{dim_key}
    """).first().n
    checks.append((f"3. Huérfanas {fk}", f"{n_orphans:,}", n_orphans == 0))

# 4. Duplicados y nulos en las llaves de las dimensiones
for dim_table, dim_key in DIM_PKS.items():
    r = spark.sql(f"""
        SELECT COUNT(*) - COUNT(DISTINCT {dim_key}) AS dups, COUNT_IF({dim_key} IS NULL) AS nulls
        FROM {dim_table}
    """).first()
    name = dim_table.split(".")[-1].strip("`")
    checks.append((f"4. Duplicados {name}", f"dups={r.dups} nulls={r.nulls}", r.dups == 0 and r.nulls == 0))

for name, detail, ok in checks:
    print(f"{'✓' if ok else '✗'} {name:<38} {detail}")
    if not ok:
        errors.append(f"{name}: {detail}")

if errors:
    # Las vistas leen fact_trips en vivo: si se deja la tabla mala, Finanzas la vería aunque el job falle.
    # Se vuelve a la versión anterior (Delta time travel) antes de fallar.
    if PREV_VERSION is not None:
        spark.sql(f"RESTORE TABLE {T_DM_FACT_TRIPS} TO VERSION AS OF {PREV_VERSION}")
        errors.append(f"fact_trips restaurada a la versión {PREV_VERSION} (datos del día anterior)")
    raise AssertionError("Conciliación del data mart FALLÓ:\n  - " + "\n  - ".join(errors))

print("\n✓ Conciliación OK: el mart cuadra con silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Informativo: viajes que quedaron en el miembro `-1`
# MAGIC
# MAGIC No es un error. Se espera que `rate_code_key = -1` coincida con los viajes Flex Fare.

# COMMAND ----------

display(spark.sql(f"""
    SELECT {", ".join(f"COUNT_IF({fk} = -1) AS {fk}_desconocido" for fk in FACT_FKS)}
    FROM {T_DM_FACT_TRIPS}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta: ¿por qué la conciliación debe hacer fallar el notebook en lugar de solo mostrar los números?
# MAGIC
# MAGIC A las 3 a. m. el notebook corre dentro de un workflow y **nadie está mirando la salida de las celdas**.
# MAGIC
# MAGIC - **Un `print` no detiene nada.** Si el conteo no cuadra, la tarea termina en verde y las tareas siguientes
# MAGIC   (constraints, vistas, dashboards) publican datos incorrectos. A las 8 a. m. Finanzas ve cifras erradas en su
# MAGIC   reporte y lo descubre un usuario, no el equipo de datos.
# MAGIC - **Un `assert` o `raise` marca la tarea como fallida.** El orquestador **no ejecuta** las tareas que dependen de
# MAGIC   ella, así que los consumidores siguen viendo los datos de ayer, que eran correctos. Además se dispara la alerta
# MAGIC   configurada (`email_notifications.on_failure` en `nyctaxi_job.yml`) y se puede reintentar.
# MAGIC - **Fallar rápido acota el daño.** El error queda en la corrida exacta donde ocurrió, con el mensaje que explica
# MAGIC   qué no cuadró, en lugar de propagarse por días hacia todo lo que se construye encima del mart.
# MAGIC - Un control que solo informa depende de que una persona lo lea. Un control que falla es **automatizable**:
# MAGIC   convierte la regla de calidad en una condición del pipeline.