# Databricks notebook source
# MAGIC %md
# MAGIC # Data mart Finanzas TLC — 01. Diseño y dimensiones
# MAGIC
# MAGIC Construye en la capa Gold un **data mart en modelo estrella** para el área de
# MAGIC Finanzas de la TLC, a partir de silver. Este notebook cubre la **Parte 0
# MAGIC (diseño)** y la **Parte 1 (dimensiones)**.
# MAGIC
# MAGIC | Notebook | Contenido |
# MAGIC |---|---|
# MAGIC | `01_dm_dimensiones` | Parte 0 (diseño) + Parte 1 (6 dimensiones) |
# MAGIC | `02_dm_fact_trips` | Parte 2 (tabla de hechos, nulos, conciliación) + diseño físico (5.3) |
# MAGIC | `03_dm_constraints_vistas_permisos` | Parte 3 (PK/FK, vistas de consumo, permisos) |
# MAGIC | `04_dm_preguntas_negocio` | Parte 4 (6 preguntas, solo con el mart) |
# MAGIC | `05_dm_rendimiento_orquestacion` | Parte 5 (silver vs estrella) + Parte 6 (DAG) |
# MAGIC
# MAGIC Requisito: haber corrido `01-bronze/` y `02-silver/`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 0. Diseño (antes de escribir código)
# MAGIC
# MAGIC ### 0.1 El grano de la tabla de hechos
# MAGIC
# MAGIC > **Cada fila de `gold_taxi_dm_fact_trips` es un viaje de taxi amarillo que pasó las reglas de calidad de silver**
# MAGIC > (una fila de `silver_yellow_trips_enriched`, ni más ni menos).
# MAGIC
# MAGIC Es el grano **más fino** disponible (grano atómico). Por eso la tabla de hechos tiene
# MAGIC exactamente las mismas filas que silver, y cualquier agregación (por mes, por borough,
# MAGIC por hora, por forma de pago…) se obtiene con un `GROUP BY` sin tener que construir
# MAGIC otra tabla.
# MAGIC
# MAGIC **¿Por qué no un grano agregado, por ejemplo "cada fila es una zona de pickup en un día"?**
# MAGIC Al agregar se **pierde para siempre** todo atributo que no esté en el grano. Con
# MAGIC "zona de pickup × día" desaparecen la hora, la zona de destino, la forma de pago, el
# MAGIC tipo de tarifa y el proveedor. De la Parte 4:
# MAGIC
# MAGIC | Pregunta | ¿Se puede con grano zona × día? | Por qué |
# MAGIC |---|---|---|
# MAGIC | 1. Revenue mensual por borough y `service_zone` de pickup | Sí | Es un *roll-up* de zona × día y las sumas son aditivas |
# MAGIC | 2. Ticket promedio hacia aeropuertos, festivo vs normal | **No** | Necesita la **zona de destino** |
# MAGIC | 3. Tarifa JFK que empieza o termina en JFK | **No** | Necesita `RatecodeID` y la **zona de destino** |
# MAGIC | 4. Propina con tarjeta por franja horaria y fin de semana | **No** | Necesita la **hora** y la **forma de pago** |
# MAGIC | 5. Mezcla de formas de pago por mes | **No** | Necesita la **forma de pago** |
# MAGIC | 6. Peso de los recargos por borough de pickup | Sí | Roll-up aditivo |
# MAGIC
# MAGIC Además, con grano agregado cada pregunta nueva obliga a rediseñar la tabla. Esto es
# MAGIC justamente lo que el mart quiere evitar. Tampoco se pueden recalcular promedios
# MAGIC correctamente si no se guardaron también las sumas y los conteos.
# MAGIC
# MAGIC ### 0.2 Diagrama del modelo estrella
# MAGIC
# MAGIC ```
# MAGIC                       ┌──────────────────────┐
# MAGIC                       │ dim_date             │
# MAGIC                       │ PK date_key (yyyyMMdd)│
# MAGIC                       │ full_date, year, ... │
# MAGIC                       │ is_holiday, ...      │
# MAGIC                       └──────────▲───────────┘
# MAGIC                                  │ pickup_date_key
# MAGIC ┌────────────────────┐   ┌───────┴──────────────────┐   ┌──────────────────────┐
# MAGIC │ dim_time           │   │ fact_trips               │   │ dim_payment_type     │
# MAGIC │ PK hour_key (0–23) ◄───┤ (grano: 1 viaje)         ├───► PK payment_type_key  │
# MAGIC │ hour_label,        │   │ FK pickup_date_key       │   │ desc, tip_is_recorded│
# MAGIC │ day_part,          │   │ FK pickup_hour_key       │   └──────────────────────┘
# MAGIC │ is_rush_hour       │   │ FK pickup_zone_key  ─┐   │   ┌──────────────────────┐
# MAGIC └────────────────────┘   │ FK dropoff_zone_key ─┤   ├───► dim_rate_code        │
# MAGIC                          │ FK payment_type_key  │   │   │ PK rate_code_key     │
# MAGIC ┌────────────────────┐   │ FK rate_code_key     │   │   └──────────────────────┘
# MAGIC │ dim_zone           │   │ FK vendor_key        │   │   ┌──────────────────────┐
# MAGIC │ PK zone_key        ◄───┤ medidas: fare_amount,│   ├───► dim_vendor           │
# MAGIC │ borough, zone,     │ ×2│ tip_amount, ...,     │   │   │ PK vendor_key        │
# MAGIC │ service_zone,      │   │ total_amount         │   │   └──────────────────────┘
# MAGIC │ is_airport,        │   └──────────────────────┘───┘
# MAGIC │ is_special_zone    │   (role-playing: pickup y dropoff usan la MISMA dim_zone)
# MAGIC └────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### 0.3 Dimensiones
# MAGIC
# MAGIC | Dimensión | Llave (PK) | Fuente | Atributos |
# MAGIC |---|---|---|---|
# MAGIC | `dim_date` | `date_key` INT `yyyyMMdd` | Generada con `sequence` entre la fecha mín. y máx. de silver + lista de festivos federales | `full_date`, `year`, `quarter`, `month`, `month_name`, `day_of_month`, `day_of_week` (1 = domingo), `day_name`, `is_weekend`, `is_holiday`, `holiday_name` |
# MAGIC | `dim_time` | `hour_key` INT 0–23 | Generada (24 filas) | `hour_label`, `day_part`, `is_rush_hour` |
# MAGIC | `dim_zone` (role-playing: pickup y dropoff) | `zone_key` INT (= `LocationID`) | `T_BRONZE_ZONES` | `borough`, `zone`, `service_zone`, `is_airport`, `is_special_zone` |
# MAGIC | `dim_payment_type` | `payment_type_key` INT (0–6, -1) | Data dictionary TLC | `payment_type_desc`, `payment_type_desc_tlc`, `payment_group`, `tip_is_recorded` |
# MAGIC | `dim_rate_code` | `rate_code_key` INT (1–6, -1) | Data dictionary TLC | `rate_code_desc`, `rate_code_desc_tlc`, `is_airport_rate` |
# MAGIC | `dim_vendor` | `vendor_key` INT (1, 2, 6, 7, -1) | Data dictionary TLC | `vendor_name`, `vendor_name_tlc` |
# MAGIC
# MAGIC ### 0.4 Medidas
# MAGIC
# MAGIC | Medida | Tipo | Comentario |
# MAGIC |---|---|---|
# MAGIC | `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee`, `cbd_congestion_fee`, `total_amount` | **Aditivas** | Dinero: se pueden sumar por cualquier dimensión (fecha, zona, pago…) |
# MAGIC | `trip_distance`, `trip_duration_min` | **Aditivas** | Millas y minutos totales recorridos tienen sentido en cualquier corte |
# MAGIC | `passenger_count` | **Aditiva** | Suma = pasajeros transportados. Es `NULL` en Flex Fare (ver notebook 02) |
# MAGIC | Conteo de viajes (`COUNT(*)`) | **Aditiva** | Es la medida implícita de un hecho de transacción |
# MAGIC | — | **Semi-aditivas** | **No hay** en esta tabla. Las semi-aditivas aparecen en tablas de *snapshot* (saldos, inventario, "taxis activos al cierre del día"), que se suman entre zonas pero **no a lo largo del tiempo**. Un viaje es una transacción, así que todo lo que mide se suma en todas las dimensiones |
# MAGIC | `tip_rate`, `avg_speed_mph`, `cost_per_mile`, ticket promedio | **No aditivas** | Son ratios. **No se guardan** en la tabla de hechos: se calculan en la consulta a partir de sus componentes aditivos |
# MAGIC
# MAGIC ### Pregunta: ¿`tip_rate` y `avg_speed_mph` deben ir como medidas en la tabla de hechos?
# MAGIC
# MAGIC **No.** Son ratios ya calculados a nivel de viaje y, por tanto, **no aditivos**:
# MAGIC
# MAGIC - **`SUM` de un ratio no significa nada.** Sumar el porcentaje de propina de 1.000 viajes da "4.000 %".
# MAGIC - **`AVG` de un ratio da un promedio simple que pondera igual cada viaje**, y no coincide con el ratio del total.
# MAGIC   Ejemplo: el viaje A tiene `fare` = 2 y `tip` = 1, así que su `tip_rate` es 50 %. El viaje B tiene `fare` = 100
# MAGIC   y `tip` = 10, con `tip_rate` de 10 %.
# MAGIC   - `AVG(tip_rate)` = (50 % + 10 %) / 2 = **30 %**.
# MAGIC   - `SUM(tip) / SUM(fare)` = 11 / 102 = **10,8 %**, que es lo que Finanzas realmente cobra en propinas por cada dólar de tarifa.
# MAGIC
# MAGIC   El viaje corto y barato infla el promedio. Lo mismo pasa con `avg_speed_mph`: la velocidad
# MAGIC   del conjunto es `SUM(trip_distance) / (SUM(trip_duration_min) / 60)`, no el promedio de las velocidades.
# MAGIC - **El ratio de silver ya trae una regla de negocio escondida.** `tip_rate` vale `0` cuando
# MAGIC   `fare_amount = 0`, y también en efectivo y Flex Fare, donde la propina simplemente no se registra.
# MAGIC   Promediarlo mezcla ceros "reales" con ceros "desconocidos".
# MAGIC
# MAGIC Por eso la tabla de hechos guarda solo los **componentes aditivos** (`tip_amount`, `fare_amount`,
# MAGIC `trip_distance`, `trip_duration_min`). Las vistas y consultas recalculan el ratio después de agregar:
# MAGIC `SUM(numerador) / SUM(denominador)`, filtrando por `tip_is_recorded` en el caso de las propinas.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ./_dm_common

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reinicio opcional del mart
# MAGIC
# MAGIC Las tablas del mart se crean con `CREATE TABLE IF NOT EXISTS` + `INSERT OVERWRITE`, para que
# MAGIC las constraints de la Parte 3 sobrevivan a cada re-ejecución. Si cambias el **esquema** de
# MAGIC alguna tabla, pon el widget `reset_mart` en `true` una sola vez. Eso borra el mart (la tabla
# MAGIC de hechos primero, por las FK) y lo recrea desde cero. Después vuelve a correr los notebooks 02 y 03.

# COMMAND ----------

dbutils.widgets.dropdown("reset_mart", "false", ["false", "true"], "Borrar y recrear el mart")
RESET_MART = dbutils.widgets.get("reset_mart") == "true"

if RESET_MART:
    for t in [T_DM_V_REVENUE_MENSUAL, T_DM_V_PROPINAS_TARJETA, T_DM_V_VIAJES]:
        spark.sql(f"DROP VIEW IF EXISTS {t}")
    spark.sql(f"DROP TABLE IF EXISTS {T_DM_FACT_TRIPS}")
    for t in DIM_PKS:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    print("⚠️  Mart borrado: se recrea a continuación")
else:
    print("Modo normal: se reemplazan los datos y se conservan las tablas y sus constraints")

# COMMAND ----------

silver = spark.table(T_SILVER_TRIPS_ENRICHED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 `dim_date`
# MAGIC
# MAGIC Se **genera** una fila por día entre la fecha mínima y la máxima de silver, con
# MAGIC `sequence` + `explode`. **No** se deriva de `DISTINCT pickup_date`.
# MAGIC
# MAGIC **Festivos federales de EE. UU.** Son los 11 *legal public holidays* de
# MAGIC [5 U.S.C. § 6103(a)](https://www.law.cornell.edu/uscode/text/5/6103), los mismos que publica la
# MAGIC [Office of Personnel Management (OPM)](https://www.opm.gov/policy-data-oversight/pay-leave/federal-holidays/).
# MAGIC Juneteenth se incluye desde 2021. La lista se arma con las reglas de la ley (fecha fija o
# MAGIC "n-ésimo lunes del mes") para cualquier año del rango cargado, así que no hay que mantenerla a mano:
# MAGIC
# MAGIC | Festivo | Regla |
# MAGIC |---|---|
# MAGIC | Año Nuevo (New Year's Day) | 1 de enero |
# MAGIC | Martin Luther King Jr. Day | 3.er lunes de enero |
# MAGIC | Día de los Presidentes (Washington's Birthday) | 3.er lunes de febrero |
# MAGIC | Memorial Day | Último lunes de mayo |
# MAGIC | Juneteenth | 19 de junio |
# MAGIC | Día de la Independencia | 4 de julio |
# MAGIC | Labor Day | 1.er lunes de septiembre |
# MAGIC | Columbus Day | 2.º lunes de octubre |
# MAGIC | Veterans Day | 11 de noviembre |
# MAGIC | Thanksgiving | 4.º jueves de noviembre |
# MAGIC | Navidad | 25 de diciembre |
# MAGIC
# MAGIC **Decisión sobre el "día observado".** Cuando un festivo de fecha fija cae en **sábado**, el gobierno lo
# MAGIC descansa el **viernes** anterior. Si cae en **domingo**, lo descansa el **lunes** siguiente (regla de la OPM).
# MAGIC Por ejemplo, el 4 de julio de 2026 es sábado y se observa el viernes 3.
# MAGIC Se marcan **ambos días**: el día real (es cuando cambia la demanda de taxis, por ejemplo los fuegos
# MAGIC artificiales del 4 de julio) y el día observado, con el sufijo `(observado)` en `holiday_name`.
# MAGIC El Inauguration Day no se incluye porque solo aplica al área de Washington D. C.

# COMMAND ----------

from datetime import date, timedelta


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """n-ésimo `weekday` (0 = lunes … 6 = domingo) del mes."""
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """Último `weekday` del mes."""
    next_month = date(year + (month == 12), month % 12 + 1, 1)
    last = next_month - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _observed(d: date) -> date:
    """Regla OPM: sábado → viernes anterior, domingo → lunes siguiente."""
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


def us_federal_holidays(year: int) -> list:
    """Lista de (fecha, nombre) con los festivos federales de un año, incluidos los días observados."""
    MON, THU = 0, 3
    fixed = [
        (date(year, 1, 1),   "Año Nuevo (New Year's Day)"),
        (date(year, 6, 19),  "Juneteenth"),
        (date(year, 7, 4),   "Día de la Independencia (Independence Day)"),
        (date(year, 11, 11), "Día de los Veteranos (Veterans Day)"),
        (date(year, 12, 25), "Navidad (Christmas Day)"),
    ]
    if year < 2021:  # Juneteenth es festivo federal desde 2021
        fixed = [h for h in fixed if h[1] != "Juneteenth"]

    floating = [
        (_nth_weekday(year, 1, MON, 3),  "Día de Martin Luther King Jr."),
        (_nth_weekday(year, 2, MON, 3),  "Día de los Presidentes (Washington's Birthday)"),
        (_last_weekday(year, 5, MON),    "Día de los Caídos (Memorial Day)"),
        (_nth_weekday(year, 9, MON, 1),  "Día del Trabajo (Labor Day)"),
        (_nth_weekday(year, 10, MON, 2), "Día de Colón (Columbus Day)"),
        (_nth_weekday(year, 11, THU, 4), "Día de Acción de Gracias (Thanksgiving)"),
    ]

    holidays = fixed + floating
    observed = [(_observed(d), f"{name} (observado)") for d, name in fixed if _observed(d) != d]
    return holidays + observed

# COMMAND ----------

bounds = silver.agg(F.min("pickup_date").alias("min_date"), F.max("pickup_date").alias("max_date")).first()
MIN_DATE, MAX_DATE = bounds.min_date, bounds.max_date
assert MIN_DATE is not None, f"{T_SILVER_TRIPS_ENRICHED} está vacía: corre primero 02-silver/"
print(f"Rango de silver: {MIN_DATE} → {MAX_DATE} ({(MAX_DATE - MIN_DATE).days + 1:,} días)")

# Se generan los años vecinos también: el Año Nuevo que cae en sábado se observa el 31 de dic del año anterior
holiday_rows = [
    (d, name)
    for y in range(MIN_DATE.year - 1, MAX_DATE.year + 2)
    for d, name in us_federal_holidays(y)
    if MIN_DATE <= d <= MAX_DATE
]
holidays_df = spark.createDataFrame(holiday_rows, "full_date DATE, holiday_name STRING")
assert holidays_df.count() == holidays_df.select("full_date").distinct().count(), "Dos festivos en el mismo día"
display(holidays_df.orderBy("full_date"))

# COMMAND ----------

MONTH_NAMES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
               "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
# Mismo orden que dayofweek(): 1 = domingo … 7 = sábado
DAY_NAMES = ["Domingo", "Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado"]

dates = spark.sql(f"""
    SELECT explode(sequence(DATE'{MIN_DATE}', DATE'{MAX_DATE}', INTERVAL 1 DAY)) AS full_date
""")

dim_date = (
    dates
    .join(F.broadcast(holidays_df), on="full_date", how="left")
    .select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.element_at(F.array(*[F.lit(m) for m in MONTH_NAMES]), F.month("full_date")).alias("month_name"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.element_at(F.array(*[F.lit(d) for d in DAY_NAMES]), F.dayofweek("full_date")).alias("day_name"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
        F.col("holiday_name").isNotNull().alias("is_holiday"),
        "holiday_name",
    )
)

write_mart_table(
    dim_date, T_DM_DIM_DATE,
    ddl="""
        date_key      INT     NOT NULL COMMENT 'Llave yyyyMMdd',
        full_date     DATE    NOT NULL,
        year          INT,
        quarter       INT,
        month         INT,
        month_name    STRING,
        day_of_month  INT,
        day_of_week   INT     COMMENT '1 = domingo … 7 = sábado (igual que pickup_dayofweek en silver)',
        day_name      STRING,
        is_weekend    BOOLEAN,
        is_holiday    BOOLEAN COMMENT 'Festivo federal de EE. UU. (día real u observado)',
        holiday_name  STRING
    """,
    comment="Dimensión calendario: una fila por día entre la fecha mínima y máxima de silver",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación `dim_date`

# COMMAND ----------

dd = spark.table(T_DM_DIM_DATE)
assert_unique_key(T_DM_DIM_DATE, "date_key")

# Una fila por día del rango, sin huecos
expected_days = (MAX_DATE - MIN_DATE).days + 1
assert dd.count() == expected_days, f"dim_date tiene {dd.count()} filas, se esperaban {expected_days}"

# Misma convención de día de la semana que silver
mismatch_dow = (
    silver.select("pickup_date", "pickup_dayofweek", F.col("is_weekend").alias("silver_is_weekend")).distinct()
    .join(dd, F.col("pickup_date") == F.col("full_date"))
    .filter((F.col("pickup_dayofweek") != F.col("day_of_week")) | (F.col("silver_is_weekend") != F.col("is_weekend")))
    .count()
)
assert mismatch_dow == 0, f"{mismatch_dow} días con day_of_week/is_weekend distinto a silver"

# Días del calendario sin ningún viaje (justamente lo que se perdería con DISTINCT pickup_date)
days_without_trips = dd.join(silver.select(F.col("pickup_date").alias("full_date")).distinct(), "full_date", "left_anti")
print(f"✓ {expected_days:,} días, convención de día de semana igual a silver")
print(f"  Días del calendario sin viajes en silver: {days_without_trips.count()}")
display(dd.filter("is_holiday").orderBy("full_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta: ¿qué se pierde si `dim_date` sale de los viajes y un día no tuvo ninguno?
# MAGIC
# MAGIC Ese día **no existiría** en la dimensión, y con él se pierden varias cosas:
# MAGIC
# MAGIC - **Los ceros desaparecen de los reportes.** Una serie diaria que parte de `dim_date LEFT JOIN fact`
# MAGIC   mostraría "0 viajes" ese día (por ejemplo, un apagón, una tormenta de nieve o un día en que falló
# MAGIC   la ingesta). Si el día no existe, la serie salta de un día al otro sin que nadie lo note, y el
# MAGIC   gráfico une los dos puntos con una línea engañosa.
# MAGIC - **Los promedios por día quedan mal calculados.** "Viajes promedio por domingo" o "revenue promedio diario
# MAGIC   del mes" dividen por los días que tuvieron viajes, no por los días del calendario. Así el promedio se infla.
# MAGIC - **No se pueden detectar huecos de datos.** Un `left_anti` de `dim_date` contra la tabla de hechos (como el
# MAGIC   de la celda anterior) es un control de calidad gratis. Si la dimensión sale de los mismos viajes, ese
# MAGIC   control es imposible.
# MAGIC - **Se pierden atributos del calendario que no dependen de los datos.** Un festivo sin viajes quedaría fuera
# MAGIC   de la lista, y la dimensión no podría usarse para fechas futuras (presupuestos, metas).
# MAGIC
# MAGIC La dimensión de fechas describe **el calendario**, no los eventos. Por eso se genera.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 `dim_time`
# MAGIC
# MAGIC Tiene 24 filas, una por hora de pickup. `is_rush_hour` usa **la misma definición que silver**:
# MAGIC 7–9 h y 17–19 h, ambos extremos incluidos (`between`).
# MAGIC
# MAGIC | `day_part` | Horas |
# MAGIC |---|---|
# MAGIC | Madrugada | 0–5 |
# MAGIC | Mañana | 6–11 |
# MAGIC | Tarde | 12–17 |
# MAGIC | Noche | 18–23 |

# COMMAND ----------

hour = F.col("id").cast("int")

dim_time = spark.range(24).select(
    hour.alias("hour_key"),
    F.format_string("%02d:00–%02d:59", hour, hour).alias("hour_label"),
    F.when(hour <= 5, "Madrugada")
     .when(hour <= 11, "Mañana")
     .when(hour <= 17, "Tarde")
     .otherwise("Noche").alias("day_part"),
    # Misma expresión que 02-silver/02_silver_trips_enriched
    (hour.between(7, 9) | hour.between(17, 19)).alias("is_rush_hour"),
)

write_mart_table(
    dim_time, T_DM_DIM_TIME,
    ddl="""
        hour_key      INT     NOT NULL COMMENT 'Hora de pickup 0–23',
        hour_label    STRING,
        day_part      STRING  COMMENT 'Madrugada 0–5, Mañana 6–11, Tarde 12–17, Noche 18–23',
        is_rush_hour  BOOLEAN COMMENT '7–9 h y 17–19 h, igual que silver'
    """,
    comment="Dimensión de hora del día: 24 filas",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación `dim_time` contra silver

# COMMAND ----------

dt = spark.table(T_DM_DIM_TIME)
assert dt.count() == 24, "dim_time debe tener 24 filas"
assert_unique_key(T_DM_DIM_TIME, "hour_key")

silver_rush = silver.select("pickup_hour", F.col("is_rush_hour").alias("silver_is_rush_hour")).distinct()

# 1) Cada hora de silver tiene un único valor de is_rush_hour
assert silver_rush.count() == silver_rush.select("pickup_hour").distinct().count(), \
    "silver tiene horas con is_rush_hour inconsistente"

# 2) Ese valor coincide con dim_time
rush_cmp = silver_rush.join(dt, F.col("pickup_hour") == F.col("hour_key"))
n_mismatch = rush_cmp.filter(F.col("silver_is_rush_hour") != F.col("is_rush_hour")).count()
assert n_mismatch == 0, f"is_rush_hour de dim_time no coincide con silver en {n_mismatch} horas"
print(f"✓ is_rush_hour coincide con silver en las {silver_rush.count()} horas presentes")

display(rush_cmp.select("hour_key", "hour_label", "day_part", "is_rush_hour", "silver_is_rush_hour").orderBy("hour_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 `dim_zone` (role-playing dimension)
# MAGIC
# MAGIC Tiene una sola tabla física y la tabla de hechos la usa **dos veces**: `pickup_zone_key` y
# MAGIC `dropoff_zone_key`. Al consultar, se une dos veces con alias distintos (`pu` para pickup, `dof` para dropoff).
# MAGIC
# MAGIC - `is_special_zone` es `true` para 264 (`Unknown`) y 265 (`Outside of NYC`). **No se borran**: son viajes
# MAGIC   reales y la tabla de hechos debe tener todas las filas de silver. Quien no las quiera las filtra con
# MAGIC   `NOT is_special_zone`, con la regla en un solo lugar.
# MAGIC - `is_airport` vale `true` para JFK, LaGuardia y Newark. Los nombres exactos se verifican abajo contra el lookup.
# MAGIC - Se agrega el miembro `-1` (`Desconocido / No aplica`), igual que en los catálogos, por si algún viaje llega
# MAGIC   sin zona. Hoy silver valida `PULocationID` y `DOLocationID` entre 1 y 265, así que no debería usarse.

# COMMAND ----------

zones_raw = spark.table(T_BRONZE_ZONES)

# Verificación de nombres exactos antes de definir is_airport
display(zones_raw.filter(F.lower("Zone").contains("airport") | F.col("LocationID").isin(SPECIAL_ZONE_IDS)))

# COMMAND ----------

def _txt(c):
    """Texto limpio; si el CSV trae vacío/null, se deja explícito."""
    return F.coalesce(F.trim(F.col(c)), F.lit("N/A"))

zones = zones_raw.select(
    F.col("LocationID").cast("int").alias("zone_key"),
    _txt("Borough").alias("borough"),
    _txt("Zone").alias("zone"),
    _txt("service_zone").alias("service_zone"),
)

dim_zone = (
    zones
    .withColumn("is_airport", F.col("zone").isin(AIRPORT_ZONE_NAMES))
    .withColumn("is_special_zone", F.col("zone_key").isin(SPECIAL_ZONE_IDS))
    .unionByName(spark.createDataFrame(
        [(UNKNOWN_KEY, UNKNOWN_DESC, UNKNOWN_DESC, UNKNOWN_DESC, False, True)],
        "zone_key INT, borough STRING, zone STRING, service_zone STRING, is_airport BOOLEAN, is_special_zone BOOLEAN",
    ))
)

write_mart_table(
    dim_zone, T_DM_DIM_ZONE,
    ddl="""
        zone_key         INT     NOT NULL COMMENT '= LocationID del lookup TLC; -1 = desconocido',
        borough          STRING,
        zone             STRING,
        service_zone     STRING,
        is_airport       BOOLEAN COMMENT 'JFK, LaGuardia o Newark',
        is_special_zone  BOOLEAN COMMENT '264 Unknown, 265 Outside of NYC y -1: excluir en rankings por zona'
    """,
    comment="Dimensión de zonas TLC. Role-playing: la usan pickup_zone_key y dropoff_zone_key",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación `dim_zone`

# COMMAND ----------

dz = spark.table(T_DM_DIM_ZONE)
assert_unique_key(T_DM_DIM_ZONE, "zone_key")

n_airports = dz.filter("is_airport").count()
assert n_airports == 3, f"Se esperaban 3 aeropuertos y hay {n_airports}: revisa AIRPORT_ZONE_NAMES contra el lookup"
assert dz.filter("is_special_zone AND zone_key > 0").count() == 2, "264 y 265 deben estar marcadas como especiales"
assert dz.count() == zones_raw.count() + 1, "dim_zone debe tener todas las zonas del lookup + el miembro -1"

# Todas las zonas que usa silver existen en la dimensión
zone_ids_silver = (
    silver.select(F.col("PULocationID").alias("z"))
    .union(silver.select(F.col("DOLocationID").alias("z"))).distinct()
)
missing_zones = zone_ids_silver.join(dz, F.col("z") == F.col("zone_key"), "left_anti").count()
assert missing_zones == 0, f"{missing_zones} zonas de silver no están en dim_zone"
print("✓ 3 aeropuertos, 2 zonas especiales, todas las zonas de silver presentes")
display(dz.filter("is_airport OR is_special_zone").orderBy("zone_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Catálogos: `dim_payment_type`, `dim_rate_code`, `dim_vendor`
# MAGIC
# MAGIC Fuente: [Data Dictionary – Yellow Taxi Trip Records](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf),
# MAGIC TLC, versión del 18 de marzo de 2025. Se guarda la descripción oficial en inglés (`*_tlc`) y una en
# MAGIC español para Finanzas.
# MAGIC
# MAGIC - **`payment_type`** 0–6. `tip_is_recorded` es `true` **solo para tarjeta (1)**. Según el diccionario, la
# MAGIC   propina "se llena automáticamente para propinas con tarjeta. Las propinas en efectivo no se incluyen".
# MAGIC   `payment_group` agrupa para la Parte 4: Tarjeta, Efectivo, Flex Fare y Otros.
# MAGIC - **`RatecodeID`** 1–6. El código 99 (`Null/unknown`) no se incluye porque silver lo rechaza. Un viaje sin
# MAGIC   tarifa (Flex Fare llega con `RatecodeID` nulo) va a `-1`.
# MAGIC - **`VendorID`** 1 y 2, que son los que silver acepta. Se agregan también 6 y 7, que el diccionario de 2025
# MAGIC   ya define. Así, si silver los acepta en el futuro, la dimensión ya los tiene y no aparecen llaves huérfanas.
# MAGIC - Cada catálogo tiene el miembro `-1` = `Desconocido / No aplica`.

# COMMAND ----------

dim_payment_type = spark.createDataFrame([
    # key, desc ES,                      desc TLC,           grupo,       tip_is_recorded
    (0,  "Viaje Flex Fare",              "Flex Fare trip",   "Flex Fare", False),
    (1,  "Tarjeta de crédito",           "Credit card",      "Tarjeta",   True),
    (2,  "Efectivo",                     "Cash",             "Efectivo",  False),
    (3,  "Sin cargo",                    "No charge",        "Otros",     False),
    (4,  "En disputa",                   "Dispute",          "Otros",     False),
    (5,  "Desconocido",                  "Unknown",          "Otros",     False),
    (6,  "Viaje anulado",                "Voided trip",      "Otros",     False),
    (UNKNOWN_KEY, UNKNOWN_DESC,          UNKNOWN_DESC,       "Otros",     False),
], "payment_type_key INT, payment_type_desc STRING, payment_type_desc_tlc STRING, payment_group STRING, tip_is_recorded BOOLEAN")

write_mart_table(
    dim_payment_type, T_DM_DIM_PAYMENT_TYPE,
    ddl="""
        payment_type_key       INT     NOT NULL COMMENT 'payment_type TLC; -1 = desconocido',
        payment_type_desc      STRING,
        payment_type_desc_tlc  STRING  COMMENT 'Descripción oficial del data dictionary TLC',
        payment_group          STRING  COMMENT 'Tarjeta / Efectivo / Flex Fare / Otros',
        tip_is_recorded        BOOLEAN COMMENT 'true solo para tarjeta: único medio donde la TLC registra la propina'
    """,
    comment="Catálogo de formas de pago (data dictionary TLC, mar-2025)",
)

# COMMAND ----------

dim_rate_code = spark.createDataFrame([
    (1, "Tarifa estándar",                "Standard rate",          False),
    (2, "Tarifa fija JFK",                "JFK",                    True),
    (3, "Newark",                         "Newark",                 True),
    (4, "Nassau o Westchester",           "Nassau or Westchester",  False),
    (5, "Tarifa negociada",               "Negotiated fare",        False),
    (6, "Viaje grupal",                   "Group ride",             False),
    (UNKNOWN_KEY, UNKNOWN_DESC,           UNKNOWN_DESC,             False),
], "rate_code_key INT, rate_code_desc STRING, rate_code_desc_tlc STRING, is_airport_rate BOOLEAN")

write_mart_table(
    dim_rate_code, T_DM_DIM_RATE_CODE,
    ddl="""
        rate_code_key       INT     NOT NULL COMMENT 'RatecodeID TLC; -1 = desconocido (p. ej. Flex Fare)',
        rate_code_desc      STRING,
        rate_code_desc_tlc  STRING  COMMENT 'Descripción oficial del data dictionary TLC',
        is_airport_rate     BOOLEAN COMMENT 'Tarifas de aeropuerto: JFK (2) y Newark (3)'
    """,
    comment="Catálogo de tipos de tarifa (data dictionary TLC, mar-2025)",
)

# COMMAND ----------

dim_vendor = spark.createDataFrame([
    (1, "Creative Mobile Technologies",   "Creative Mobile Technologies, LLC"),
    (2, "Curb Mobility",                  "Curb Mobility, LLC"),
    (6, "Myle Technologies",              "Myle Technologies Inc"),
    (7, "Helix",                          "Helix"),
    (UNKNOWN_KEY, UNKNOWN_DESC,           UNKNOWN_DESC),
], "vendor_key INT, vendor_name STRING, vendor_name_tlc STRING")

write_mart_table(
    dim_vendor, T_DM_DIM_VENDOR,
    ddl="""
        vendor_key       INT     NOT NULL COMMENT 'VendorID TLC; -1 = desconocido',
        vendor_name      STRING,
        vendor_name_tlc  STRING  COMMENT 'Nombre oficial del data dictionary TLC'
    """,
    comment="Catálogo de proveedores de tecnología (data dictionary TLC, mar-2025)",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación de los catálogos: cubren todos los códigos de silver

# COMMAND ----------

for t, k in [(T_DM_DIM_PAYMENT_TYPE, "payment_type_key"), (T_DM_DIM_RATE_CODE, "rate_code_key"), (T_DM_DIM_VENDOR, "vendor_key")]:
    assert_unique_key(t, k)
    assert spark.table(t).filter(F.col(k) == UNKNOWN_KEY).count() == 1, f"{t} no tiene miembro -1"

assert spark.table(T_DM_DIM_PAYMENT_TYPE).filter("tip_is_recorded").select("payment_type_key").first()[0] == 1
assert spark.table(T_DM_DIM_PAYMENT_TYPE).filter("tip_is_recorded").count() == 1

def _uncovered(silver_col, dim_table, key):
    codes = silver.select(F.coalesce(F.col(silver_col).cast("int"), F.lit(UNKNOWN_KEY)).alias("code")).distinct()
    return [r.code for r in codes.join(spark.table(dim_table), codes.code == F.col(key), "left_anti").collect()]

for silver_col, dim_table, key in [("payment_type", T_DM_DIM_PAYMENT_TYPE, "payment_type_key"),
                                   ("RatecodeID",   T_DM_DIM_RATE_CODE,    "rate_code_key"),
                                   ("VendorID",     T_DM_DIM_VENDOR,       "vendor_key")]:
    missing = _uncovered(silver_col, dim_table, key)
    assert not missing, f"Códigos de {silver_col} en silver que no están en {dim_table}: {missing}"
    print(f"✓ {silver_col}: todos los códigos de silver están en la dimensión")

display(spark.table(T_DM_DIM_PAYMENT_TYPE).orderBy("payment_type_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta: ¿por qué un miembro `-1` es mejor que dejar la llave foránea en `NULL`?
# MAGIC
# MAGIC 1. **Los `INNER JOIN` no pierden filas en silencio.** `NULL = NULL` no es verdadero en SQL, así que un viaje
# MAGIC    con `rate_code_key = NULL` desaparece de cualquier `INNER JOIN` con `dim_rate_code`. Todos los viajes Flex Fare
# MAGIC    se esfumarían de los reportes de revenue y nadie lo notaría. Con `-1`, el join siempre encuentra pareja y
# MAGIC    `SUM(total_amount)` del reporte cuadra con el total del mart.
# MAGIC 2. **El reporte muestra un grupo con nombre.** En vez de una fila con `NULL` que el usuario de Finanzas no sabe
# MAGIC    interpretar, aparece "Desconocido / No aplica", con su número de viajes y su revenue. El faltante queda visible y medible.
# MAGIC 3. **Integridad referencial sin excepciones.** Las llaves foráneas pueden declararse `NOT NULL`, y "cero llaves huérfanas"
# MAGIC    se verifica igual para todas las columnas. Con `NULL` habría que tratar ese caso aparte en cada control y en cada consulta.
# MAGIC 4. **Distingue "no aplica" de "error de carga".** Un `NULL` puede significar que el dato no existe o que el join falló.
# MAGIC    El `-1` es una decisión explícita del ETL. Si se quiere más detalle, se pueden agregar otros miembros, como `-2 = No aplica`.
# MAGIC 5. **Los filtros de BI funcionan.** Muchas herramientas manejan mal los `NULL` en filtros y segmentaciones. Un valor
# MAGIC    con descripción se puede seleccionar o excluir como cualquier otro.