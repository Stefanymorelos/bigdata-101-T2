# Databricks notebook source
# MAGIC %md
# MAGIC # Data mart Finanzas TLC — 05. Rendimiento, diseño físico y orquestación
# MAGIC
# MAGIC Cubre la **Parte 5** (la misma consulta sobre silver y sobre la estrella, planes y diseño físico) y la
# MAGIC **Parte 6** (DAG del workflow).
# MAGIC
# MAGIC Los tiempos se midieron en el mismo cómputo (*Standard Cluster*) para las dos versiones, sin otros notebooks corriendo.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F
import time

# COMMAND ----------

# MAGIC %run ./_dm_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 La misma consulta, dos versiones
# MAGIC
# MAGIC > Top 20 pares zona de origen → zona de destino con más viajes, excluyendo las zonas 264 y 265 en ambos extremos.
# MAGIC > Para cada par: viajes, distancia promedio, `total_amount` promedio y duración promedio.
# MAGIC
# MAGIC Se agrupa por **ID y nombre** de zona en las dos versiones. Algunos nombres de zona se repiten en el lookup con IDs
# MAGIC distintos (por ejemplo, *Governor's Island/Ellis Island/Liberty Island* o *Corona*). Si se agrupara solo por nombre,
# MAGIC se mezclarían zonas distintas.
# MAGIC El desempate (`ORDER BY viajes DESC, ids`) hace que el top 20 sea determinístico aunque haya empates en el puesto 20.

# COMMAND ----------

Q_SILVER = f"""
SELECT PULocationID                       AS pickup_zone_id,
       pickup_zone,
       DOLocationID                       AS dropoff_zone_id,
       dropoff_zone,
       COUNT(*)                           AS viajes,
       ROUND(AVG(trip_distance), 2)       AS distancia_promedio,
       ROUND(AVG(total_amount), 2)        AS total_promedio,
       ROUND(AVG(trip_duration_min), 2)   AS duracion_promedio
FROM {T_SILVER_TRIPS_ENRICHED}
WHERE PULocationID NOT IN (264, 265)
  AND DOLocationID NOT IN (264, 265)
GROUP BY PULocationID, pickup_zone, DOLocationID, dropoff_zone
ORDER BY viajes DESC, pickup_zone_id, dropoff_zone_id
LIMIT 20
"""

Q_STAR = f"""
SELECT pu.zone_key                        AS pickup_zone_id,
       pu.zone                            AS pickup_zone,
       dof.zone_key                       AS dropoff_zone_id,
       dof.zone                           AS dropoff_zone,
       COUNT(*)                           AS viajes,
       ROUND(AVG(f.trip_distance), 2)     AS distancia_promedio,
       ROUND(AVG(f.total_amount), 2)      AS total_promedio,
       ROUND(AVG(f.trip_duration_min), 2) AS duracion_promedio
FROM {T_DM_FACT_TRIPS} f
JOIN {T_DM_DIM_ZONE} pu  ON f.pickup_zone_key  = pu.zone_key
JOIN {T_DM_DIM_ZONE} dof ON f.dropoff_zone_key = dof.zone_key
WHERE NOT pu.is_special_zone
  AND NOT dof.is_special_zone
GROUP BY pu.zone_key, pu.zone, dof.zone_key, dof.zone
ORDER BY viajes DESC, pickup_zone_id, dropoff_zone_id
LIMIT 20
"""

# COMMAND ----------

res_silver = spark.sql(Q_SILVER)
res_star = spark.sql(Q_STAR)
display(res_star)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación: ambas versiones devuelven el mismo resultado
# MAGIC
# MAGIC Los pares, los nombres y los conteos deben ser **idénticos**. En los promedios se acepta ±0,01, porque sumar
# MAGIC `DOUBLE` en otro orden puede mover el último decimal al redondear.

# COMMAND ----------

KEYS = ["pickup_zone_id", "pickup_zone", "dropoff_zone_id", "dropoff_zone", "viajes"]
AVGS = ["distancia_promedio", "total_promedio", "duracion_promedio"]

s = res_silver.withColumn("pickup_zone_id", F.col("pickup_zone_id").cast("int")) \
              .withColumn("dropoff_zone_id", F.col("dropoff_zone_id").cast("int"))
cmp = s.alias("s").join(res_star.alias("e"), on=KEYS, how="full_outer")

n_only_one_side = cmp.filter(" OR ".join(f"s.{c} IS NULL OR e.{c} IS NULL" for c in AVGS)).count()
n_diff = cmp.filter(" OR ".join(f"abs(s.{c} - e.{c}) > 0.011" for c in AVGS)).count()

assert res_silver.count() == res_star.count() == 20, "Las dos versiones deben devolver 20 filas"
assert n_only_one_side == 0, f"{n_only_one_side} pares aparecen en una sola versión"
assert n_diff == 0, f"{n_diff} pares con promedios distintos"
print("✓ Silver y estrella devuelven los mismos 20 pares, con los mismos conteos y promedios")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Comparación: tiempo y plan
# MAGIC
# MAGIC Cada consulta se ejecuta **4 veces** y se descarta la primera, que es de calentamiento: compila, lee metadatos y
# MAGIC llena el disk cache. Se reporta la mediana de las 3 restantes. Se usa `.collect()` para forzar la ejecución completa,
# MAGIC porque `display` solo trae las primeras filas.

# COMMAND ----------

# Opcional: desactivar el disk cache para medir lecturas reales (en serverless no se permite y se ignora)
try:
    spark.conf.set("spark.databricks.io.cache.enabled", "false")
    print("Disk cache desactivado para la medición")
except Exception as e:
    print(f"No se pudo desactivar el disk cache ({type(e).__name__}): se mide con la configuración por defecto")


def time_query(sql: str, runs: int = 4) -> list:
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        spark.sql(sql).collect()
        times.append(time.perf_counter() - t0)
    return times


t_silver = time_query(Q_SILVER)
t_star = time_query(Q_STAR)

def _median(xs):
    xs = sorted(xs)
    return xs[len(xs) // 2]

timing = spark.createDataFrame(
    [("Silver (tabla ancha)", *[round(x, 2) for x in t_silver], round(_median(t_silver[1:]), 2)),
     ("Estrella (fact + 2× dim_zone)", *[round(x, 2) for x in t_star], round(_median(t_star[1:]), 2))],
    "version STRING, run1_calentamiento DOUBLE, run2 DOUBLE, run3 DOUBLE, run4 DOUBLE, mediana_s DOUBLE",
)
display(timing)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Planes de ejecución (`EXPLAIN FORMATTED`)
# MAGIC
# MAGIC Se cuentan los nodos de join y de broadcast. Con Photon, los nodos se llaman `PhotonBroadcastHashJoin` y
# MAGIC `PhotonShuffleExchange…`. `EXPLAIN` muestra el plan **inicial**. El plan **final**, que puede cambiar por AQE (Adaptive
# MAGIC Query Execution), se ve en la Spark UI: pestaña **SQL / DataFrame**, abriendo la consulta en el grafo de la ejecución.

# COMMAND ----------

def plan(sql: str) -> str:
    return spark.sql(f"EXPLAIN FORMATTED {sql}").first()[0]

import re

def plan_summary(name: str, p: str) -> dict:
    # En EXPLAIN FORMATTED cada operador aparece una vez como "(n) NombreOperador" en la sección de detalles
    ops = re.findall(r"^\(\d+\) (\w+)", p, flags=re.M)
    count = lambda pat: sum(1 for op in ops if pat in op)
    return {
        "version": name,
        "broadcast_hash_joins": count("BroadcastHashJoin"),
        "broadcast_exchanges": count("BroadcastExchange"),
        "sort_merge_joins": count("SortMergeJoin"),
        "shuffle_exchanges": count("ShuffleExchange"),
        "scans": count("Scan"),
    }

plan_silver, plan_star = plan(Q_SILVER), plan(Q_STAR)
display(spark.createDataFrame([plan_summary("Silver", plan_silver), plan_summary("Estrella", plan_star)]))

# COMMAND ----------

print("═══ PLAN SILVER ═══")
print(plan_silver)

# COMMAND ----------

print("═══ PLAN ESTRELLA ═══")
print(plan_star)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿Dónde aparecen los broadcast joins?
# MAGIC
# MAGIC - **Estrella:** en el plan de la consulta hay **dos** `BroadcastHashJoin`, uno por cada rol de `dim_zone` (pickup y dropoff).
# MAGIC   `dim_zone` tiene 266 filas, muy por debajo del umbral de broadcast (`spark.sql.autoBroadcastJoinThreshold`,
# MAGIC   10 MB por defecto), así que Spark la envía completa a cada ejecutor (`BroadcastExchange`) y cada tarea une su
# MAGIC   pedazo de `fact_trips` en memoria, **sin shuffle** de la tabla grande. No hace falta la pista `F.broadcast()`:
# MAGIC   el optimizador lo decide solo con las estadísticas.
# MAGIC   El filtro `NOT is_special_zone` se aplica en la dimensión antes del broadcast. Con *dynamic file pruning* puede
# MAGIC   incluso ayudar a saltar archivos de `fact_trips`.
# MAGIC - **Silver:** el plan **no tiene ningún join**. El broadcast join con las zonas ya ocurrió **una vez, al escribir**
# MAGIC   `silver_yellow_trips_enriched` (`F.broadcast(zones_pickup)` y `F.broadcast(zones_dropoff)` en
# MAGIC   `02_silver_trips_enriched`). La tabla ancha "pre-paga" el join al materializar los nombres de zona en cada fila.
# MAGIC - **En las dos versiones** el costo dominante es el mismo: un scan de todas las filas, un `HashAggregate` parcial,
# MAGIC   un shuffle por la llave del `GROUP BY` y el `TakeOrderedAndProject` del top 20.
# MAGIC
# MAGIC ✏️ **Mis tiempos:** silver **__ s** vs. estrella **__ s** (mediana).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta: ¿la estrella fue más rápida que la tabla ancha de silver? Si no, ¿por qué? ¿Qué ganas con el mart aunque no gane en velocidad?
# MAGIC
# MAGIC ✏️ *Resultado observado: ______.*
# MAGIC
# MAGIC Lo esperable es que los tiempos sean **parecidos**, y no es raro que silver gane:
# MAGIC
# MAGIC - **Silver ya tiene el join hecho.** La estrella agrega dos broadcast joins por consulta. Son baratos (una búsqueda en
# MAGIC   una tabla hash de 266 filas por viaje), pero no son gratis.
# MAGIC - **El formato columnar le quita la ventaja a la tabla "angosta".** Parquet/Delta solo lee las columnas que la consulta
# MAGIC   usa, así que las ~40 columnas extra de silver **no se leen**. Las dos versiones leen 5–6 columnas de la misma cantidad de filas.
# MAGIC - Lo que **puede** inclinar la balanza hacia la estrella es el **diseño físico**, no el modelo. `fact_trips` tiene
# MAGIC   llaves `INT` y archivos compactados con liquid clustering. `silver_yellow_trips_enriched` está particionada por
# MAGIC   `pickup_date`: más de 1.000 carpetas con archivos pequeños, y más tareas y metadatos que abrir. En una consulta
# MAGIC   sin filtro de fecha como esta, esa partición no se aprovecha y solo agrega overhead.
# MAGIC
# MAGIC **Lo que se gana con el mart aunque no gane en velocidad:**
# MAGIC
# MAGIC 1. **Reglas de negocio en un solo lugar.** `NOT IN (264, 265)` está copiado en `01_gold_revenue_by_zone`,
# MAGIC    `03_gold_hourly_demand` y `05_top_trip_pairs.sql`. En el mart es `is_special_zone`, y lo mismo pasa con
# MAGIC    `tip_is_recorded`, `is_airport`, los festivos y el `-1`. Cambiar la regla es cambiar una fila de una dimensión.
# MAGIC 2. **Flexibilidad.** Una pregunta nueva es una consulta nueva, no una tabla, un notebook y una tarea nuevos en el workflow.
# MAGIC 3. **Gobierno y seguridad.** Finanzas consulta vistas con nombres legibles y **no tiene acceso a silver**. La vista es
# MAGIC    el contrato.
# MAGIC 4. **Atributos que silver no tiene.** Festivos, `day_part`, descripciones de pago y tarifa, y `payment_group` se agregan
# MAGIC    una vez en la dimensión y quedan disponibles para todas las preguntas.
# MAGIC 5. **Consistencia verificada.** La conciliación garantiza que el mart cuadra con silver. Las PK/FK documentan el modelo
# MAGIC    para las herramientas de BI y, con `RELY`, permiten eliminar joins innecesarios.
# MAGIC 6. **Estabilidad.** Si silver cambia (columnas renombradas, nuevas fuentes), el ETL del mart absorbe el cambio y las
# MAGIC    consultas de Finanzas no se rompen.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta: ¿cuándo seguirías creando una tabla agregada (por ejemplo, demanda por zona × fecha × hora consultada cientos de veces al día) aunque tengas el mart?
# MAGIC
# MAGIC Cuando **la misma agregación se lee muchísimas más veces de las que cambian los datos**, y el costo o la latencia de
# MAGIC recalcularla desde el grano de viaje no se justifica:
# MAGIC
# MAGIC - **Dashboards de alta frecuencia con SLA de latencia.** Un heatmap que cientos de usuarios abren al día. Recorrer
# MAGIC   decenas de millones de viajes en cada carga cuesta tiempo y dinero. Una tabla de zona × fecha × hora tiene unos
# MAGIC   cientos de miles de filas y responde en milisegundos.
# MAGIC - **Volumen muy grande o cómputo pequeño** (SQL warehouse chico, muchos usuarios simultáneos).
# MAGIC - **Herramientas que no hacen joins bien** o integraciones externas que esperan una tabla plana.
# MAGIC
# MAGIC Pero esa tabla **se construye desde el mart, no desde silver**. Es una *tabla de hechos agregada* (*aggregate fact
# MAGIC table*, en términos de Kimball) que reusa las mismas llaves y dimensiones. Por ejemplo,
# MAGIC `SELECT pickup_date_key, pickup_hour_key, pickup_zone_key, COUNT(*), SUM(...) FROM fact_trips GROUP BY ...`.
# MAGIC Así sigue las mismas reglas de negocio (`is_special_zone`, festivos) y sus cifras cuadran con el mart. Debe guardar
# MAGIC **solo medidas aditivas** (conteos y sumas) para que se pueda re-agregar. En Databricks se puede implementar como
# MAGIC **vista materializada**, que se refresca de forma incremental, o como una tarea más del DAG que depende de `dm_fact_trips`.
# MAGIC
# MAGIC ## 5.3 Diseño físico de `fact_trips`
# MAGIC
# MAGIC Está aplicado y justificado en `02_dm_fact_trips` (sección 2.3): **liquid clustering
# MAGIC `CLUSTER BY (pickup_date_key, pickup_zone_key)`**, sin particionado ni `ZORDER`, porque son incompatibles con liquid
# MAGIC clustering. En resumen: la fecha es el filtro de casi todos los reportes financieros (preguntas 1, 2, 4 y 5) y la zona
# MAGIC de pickup el segundo (preguntas 1 y 6). Particionar por día generaría *small files*, y liquid clustering es incremental
# MAGIC y permite cambiar las llaves sin reescribir la tabla.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {T_DM_FACT_TRIPS}").select("name", "clusteringColumns", "partitionColumns", "numFiles", "sizeInBytes"))
display(spark.sql(f"DESCRIBE DETAIL {T_SILVER_TRIPS_ENRICHED}").select("name", "clusteringColumns", "partitionColumns", "numFiles", "sizeInBytes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 6. Orquestación: DAG del mart en `07-workflow/resources/nyctaxi_job.yml`
# MAGIC
# MAGIC ```
# MAGIC  bronze_trips        bronze_zones
# MAGIC       │                 │   │
# MAGIC       ▼                 │   │
# MAGIC  silver_clean           │   │
# MAGIC       │                 │   │
# MAGIC       ▼                 │   │
# MAGIC  silver_enriched ◄──────┘   │
# MAGIC       │                     │
# MAGIC       ├──────────────┬──────┼──────────────┬───────────────────┬──────────────────┐
# MAGIC       ▼              ▼      ▼              ▼                   ▼                  ▼
# MAGIC  gold_revenue   gold_daily  dm_dimensions  gold_hourly_demand  gold_ml_features   (en paralelo)
# MAGIC  _by_zone       _metrics     (01)                                    │
# MAGIC                              │                                       ▼
# MAGIC                              ▼                                     train → register → batch_inference
# MAGIC                         dm_fact_trips (02)
# MAGIC                         └ última celda: CONCILIACIÓN
# MAGIC                           (falla → RESTORE + error → se corta aquí)
# MAGIC                              │
# MAGIC                              ▼
# MAGIC                         dm_constraints_views (03)
# MAGIC                         (PK RELY, FK, vistas, grants)
# MAGIC ```
# MAGIC
# MAGIC - **Dependencias:** `dm_dimensions` depende de `silver_enriched` (para el rango de `dim_date`) y de `bronze_zones`
# MAGIC   (fuente de `dim_zone`). `dm_fact_trips` necesita las dimensiones para resolver las llaves, y `dm_constraints_views`
# MAGIC   necesita la tabla de hechos ya conciliada.
# MAGIC - **En paralelo:** la rama del mart corre al mismo tiempo que las cuatro tareas `gold_*` existentes. Todas dependen
# MAGIC   solo de `silver_enriched` y no se leen entre sí.
# MAGIC - **Conciliación:** es la **última celda de `dm_fact_trips`**. Si falla, restaura la versión anterior de la tabla de
# MAGIC   hechos y lanza el error. La tarea queda en rojo, `dm_constraints_views` **no corre** (no se re-declara `RELY` sobre
# MAGIC   datos no verificados) y se dispara el correo `on_failure`.
# MAGIC - Si las dimensiones crecieran, `dm_dimensions` se podría partir en tareas paralelas (`dim_date`, `dim_time`,
# MAGIC   `dim_zone`, catálogos), todas antes de `dm_fact_trips`.
# MAGIC - `04_dm_preguntas_negocio` y `05_dm_rendimiento_orquestacion` son de análisis y **no** van en el job.
# MAGIC
# MAGIC Fragmento YAML que se agregaría bajo `tasks:` (no se modifica el bundle):
# MAGIC
# MAGIC ```yaml
# MAGIC         # -------- Gold: data mart Finanzas (modelo estrella) ----------
# MAGIC         - task_key: dm_dimensions
# MAGIC           job_cluster_key: nyctaxi_cluster
# MAGIC           depends_on:
# MAGIC             - task_key: silver_enriched
# MAGIC             - task_key: bronze_zones
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../08-gold-datamart/01_dm_dimensiones
# MAGIC
# MAGIC         - task_key: dm_fact_trips          # incluye la conciliación (falla el job si no cuadra)
# MAGIC           job_cluster_key: nyctaxi_cluster
# MAGIC           depends_on:
# MAGIC             - task_key: dm_dimensions
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../08-gold-datamart/02_dm_fact_trips
# MAGIC
# MAGIC         - task_key: dm_constraints_views
# MAGIC           job_cluster_key: nyctaxi_cluster
# MAGIC           depends_on:
# MAGIC             - task_key: dm_fact_trips
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../08-gold-datamart/03_dm_constraints_vistas_permisos
# MAGIC ```