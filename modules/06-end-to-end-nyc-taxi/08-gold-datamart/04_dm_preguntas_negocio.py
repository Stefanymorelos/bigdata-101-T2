# Databricks notebook source
# MAGIC %md
# MAGIC # Data mart Finanzas TLC — 04. Preguntas de negocio
# MAGIC
# MAGIC **Parte 4** del ejercicio. Todas las preguntas se responden **solo con el mart**: las vistas de consumo o
# MAGIC joins sobre la estrella. **No se toca silver.**
# MAGIC
# MAGIC Fíjate en que ninguna consulta repite reglas de negocio con códigos (`payment_type = 1`, `NOT IN (264, 265)`,
# MAGIC `RatecodeID = 2`…). Las reglas viven en las dimensiones (`tip_is_recorded`, `is_special_zone`, `is_airport`,
# MAGIC `rate_code_desc`) y las consultas solo las usan.
# MAGIC
# MAGIC Periodo analizado: **octubre de 2024 a marzo de 2025** (19.909.275 viajes en el mart).

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ./_dm_common

# COMMAND ----------

FACT, D_DATE, D_TIME, D_ZONE = T_DM_FACT_TRIPS, T_DM_DIM_DATE, T_DM_DIM_TIME, T_DM_DIM_ZONE
D_PAY, D_RATE = T_DM_DIM_PAYMENT_TYPE, T_DM_DIM_RATE_CODE

DAY_PART_ORDER = "CASE franja_dia WHEN 'Madrugada' THEN 1 WHEN 'Mañana' THEN 2 WHEN 'Tarde' THEN 3 ELSE 4 END"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pregunta 1. Revenue mensual por borough y `service_zone` de pickup. ¿Qué participación tienen los aeropuertos?
# MAGIC
# MAGIC **1a.** Revenue mensual por borough y `service_zone`, con su participación dentro del mes. Sale directamente de la
# MAGIC vista `v_revenue_mensual`, que es aditiva.

# COMMAND ----------

display(spark.sql(f"""
    SELECT anio, mes, nombre_mes, borough_pickup, service_zone_pickup,
           viajes,
           revenue,
           ROUND(100 * revenue / SUM(revenue) OVER (PARTITION BY anio, mes), 2) AS pct_revenue_mes
    FROM {T_DM_V_REVENUE_MENSUAL}
    ORDER BY anio, mes, revenue DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **1b.** Participación de los aeropuertos, medida de dos formas:
# MAGIC - **Pickup en aeropuerto:** el viaje empieza en JFK, LaGuardia o Newark. Es la misma dimensión de la pregunta 1a.
# MAGIC - **Toca un aeropuerto:** el viaje empieza **o** termina en uno. Muestra el peso real del negocio de aeropuertos,
# MAGIC   porque muchos viajes van **hacia** el aeropuerto.

# COMMAND ----------

airport_share = spark.sql(f"""
    SELECT d.year AS anio, d.month AS mes,
           COUNT(*)                                                         AS viajes,
           ROUND(SUM(f.total_amount), 2)                                    AS revenue,
           ROUND(100 * SUM(f.total_amount) FILTER (WHERE pu.is_airport)
                     / SUM(f.total_amount), 2)                              AS pct_revenue_pickup_aeropuerto,
           ROUND(100 * SUM(f.total_amount) FILTER (WHERE pu.is_airport OR dof.is_airport)
                     / SUM(f.total_amount), 2)                              AS pct_revenue_toca_aeropuerto,
           ROUND(100 * COUNT_IF(pu.is_airport OR dof.is_airport) / COUNT(*), 2) AS pct_viajes_toca_aeropuerto
    FROM {FACT} f
    JOIN {D_DATE} d   ON f.pickup_date_key  = d.date_key
    JOIN {D_ZONE} pu  ON f.pickup_zone_key  = pu.zone_key
    JOIN {D_ZONE} dof ON f.dropoff_zone_key = dof.zone_key
    GROUP BY d.year, d.month
    ORDER BY anio, mes
""")
display(airport_share)

# COMMAND ----------

# Total del periodo, por aeropuerto (rol pickup)
display(spark.sql(f"""
    SELECT COALESCE(pu.zone, 'TOTAL aeropuertos') AS aeropuerto_pickup,
           COUNT(*)                                AS viajes,
           ROUND(SUM(f.total_amount), 2)           AS revenue,
           ROUND(100 * SUM(f.total_amount) / (SELECT SUM(total_amount) FROM {FACT}), 2) AS pct_revenue_total
    FROM {FACT} f
    JOIN {D_ZONE} pu ON f.pickup_zone_key = pu.zone_key
    WHERE pu.is_airport
    GROUP BY ROLLUP (pu.zone)
    ORDER BY revenue DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretación 1**
# MAGIC
# MAGIC - Manhattan (`service_zone = Yellow Zone`) concentra la mayor parte de los viajes y del revenue: es el mercado
# MAGIC   natural del taxi amarillo.
# MAGIC - Los aeropuertos (`service_zone = Airports` para JFK y LaGuardia, `EWR` para Newark) aportan pocos viajes, pero
# MAGIC   una participación en revenue **mucho mayor** que su participación en viajes. Son viajes largos, con tarifa fija
# MAGIC   a JFK, peajes y `airport_fee`. Esto se ve al comparar `pct_revenue_toca_aeropuerto` con `pct_viajes_toca_aeropuerto`.
# MAGIC - ✏️ En el periodo, los viajes que **empiezan** en un aeropuerto representan **__ %** del revenue, y los que
# MAGIC   **tocan** un aeropuerto, **__ %**. JFK aporta **__ %** y LaGuardia **__ %**. Newark es marginal porque está en
# MAGIC   Nueva Jersey y los taxis amarillos no recogen pasajeros allí de forma habitual.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pregunta 2. Ticket promedio de los viajes que terminan en un aeropuerto: festivos vs. días normales
# MAGIC
# MAGIC "Terminan en un aeropuerto" usa `dim_zone` en el **rol dropoff**. El ticket promedio es `SUM(total_amount) / COUNT(*)`.
# MAGIC Se agrega `viajes_por_dia` porque los festivos son pocos días: así se compara la intensidad de la demanda, no solo
# MAGIC el volumen.

# COMMAND ----------

display(spark.sql(f"""
    SELECT CASE WHEN d.is_holiday THEN 'Festivo' ELSE 'Día normal' END AS tipo_dia,
           COUNT(DISTINCT d.date_key)                         AS dias,
           COUNT(*)                                           AS viajes,
           ROUND(COUNT(*) / COUNT(DISTINCT d.date_key), 0)    AS viajes_por_dia,
           ROUND(SUM(f.total_amount) / COUNT(*), 2)           AS ticket_promedio,
           ROUND(SUM(f.trip_distance) / COUNT(*), 2)          AS distancia_promedio
    FROM {FACT} f
    JOIN {D_DATE} d   ON f.pickup_date_key  = d.date_key
    JOIN {D_ZONE} dof ON f.dropoff_zone_key = dof.zone_key
    WHERE dof.is_airport
    GROUP BY d.is_holiday
    ORDER BY tipo_dia
"""))

# COMMAND ----------

# Detalle: por aeropuerto de destino, y separando fines de semana (control: varios festivos reales caen en fin de semana)
display(spark.sql(f"""
    SELECT dof.zone AS aeropuerto_destino,
           CASE WHEN d.is_holiday THEN 'Festivo'
                WHEN d.is_weekend THEN 'Fin de semana (no festivo)'
                ELSE 'Entre semana (no festivo)' END   AS tipo_dia,
           COUNT(*)                                    AS viajes,
           ROUND(SUM(f.total_amount) / COUNT(*), 2)    AS ticket_promedio
    FROM {FACT} f
    JOIN {D_DATE} d   ON f.pickup_date_key  = d.date_key
    JOIN {D_ZONE} dof ON f.dropoff_zone_key = dof.zone_key
    WHERE dof.is_airport
    GROUP BY dof.zone, tipo_dia
    ORDER BY aeropuerto_destino, tipo_dia
"""))

# COMMAND ----------

# Por festivo: cuáles mueven más viajes al aeropuerto
display(spark.sql(f"""
    SELECT d.holiday_name AS festivo,
           COUNT(DISTINCT d.date_key)                       AS dias,
           ROUND(COUNT(*) / COUNT(DISTINCT d.date_key), 0)  AS viajes_por_dia,
           ROUND(SUM(f.total_amount) / COUNT(*), 2)         AS ticket_promedio
    FROM {FACT} f
    JOIN {D_DATE} d   ON f.pickup_date_key  = d.date_key
    JOIN {D_ZONE} dof ON f.dropoff_zone_key = dof.zone_key
    WHERE dof.is_airport AND d.is_holiday
    GROUP BY d.holiday_name
    ORDER BY viajes_por_dia DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretación 2**
# MAGIC
# MAGIC - ✏️ Ticket promedio hacia aeropuertos: **$__** en festivos vs. **$__** en días normales (diferencia de **__ %**).
# MAGIC - Qué esperar: en un festivo hay **menos tráfico**, así que el taxímetro corre menos tiempo en los tramos
# MAGIC   cobrados por tiempo. Además, no hay hora pico (sin el recargo `extra` de hora pico de lunes a viernes), y la mezcla
# MAGIC   de viajes cambia (más turistas y familias, menos viajes de negocios). El ticket puede ser algo **menor o similar**.
# MAGIC   Los viajes a JFK desde Manhattan tienen tarifa fija y no cambian.
# MAGIC - El control por fin de semana sirve para separar el efecto "festivo" del efecto "no es día laboral".
# MAGIC   Si el festivo se parece más al fin de semana que al día entre semana, el driver es la ausencia de tráfico y de
# MAGIC   recargos de hora pico.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pregunta 3. De los viajes con tarifa JFK (`RatecodeID = 2`), ¿qué porcentaje empieza o termina en JFK Airport? ¿Qué explica el resto?
# MAGIC
# MAGIC La tarifa se filtra por su descripción en `dim_rate_code` (`rate_code_desc_tlc = 'JFK'`) y el aeropuerto por su
# MAGIC nombre en `dim_zone`, en sus dos roles.

# COMMAND ----------

jfk = spark.sql(f"""
    SELECT f.*, pu.zone AS pu_zone, pu.borough AS pu_borough, pu.is_special_zone AS pu_special, pu.is_airport AS pu_airport,
           dof.zone AS do_zone, dof.borough AS do_borough, dof.is_special_zone AS do_special, dof.is_airport AS do_airport
    FROM {FACT} f
    JOIN {D_RATE} r   ON f.rate_code_key    = r.rate_code_key
    JOIN {D_ZONE} pu  ON f.pickup_zone_key  = pu.zone_key
    JOIN {D_ZONE} dof ON f.dropoff_zone_key = dof.zone_key
    WHERE r.rate_code_desc_tlc = 'JFK'
""")
jfk.createOrReplaceTempView("jfk_rate_trips")

display(spark.sql("""
    SELECT COUNT(*)                                                                   AS viajes_tarifa_jfk,
           COUNT_IF(pu_zone = 'JFK Airport' OR do_zone = 'JFK Airport')                AS empiezan_o_terminan_en_jfk,
           ROUND(100 * COUNT_IF(pu_zone = 'JFK Airport' OR do_zone = 'JFK Airport') / COUNT(*), 2) AS pct_jfk
    FROM jfk_rate_trips
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Qué explica el resto?** Se clasifica cada viaje con tarifa JFK que **no** toca la zona JFK Airport:

# COMMAND ----------

display(spark.sql("""
    SELECT CASE
             WHEN pu_zone = 'JFK Airport' OR do_zone = 'JFK Airport'      THEN '0. Empieza o termina en JFK'
             WHEN pu_special OR do_special                                 THEN '1. Zona desconocida o fuera de NYC (264/265): ubicación no asignada'
             WHEN pu_airport OR do_airport                                 THEN '2. Otro aeropuerto (LaGuardia/Newark): tarifa mal registrada'
             WHEN pu_borough = 'Queens' OR do_borough = 'Queens'           THEN '3. Queens cerca de JFK (zonas vecinas, p. ej. Jamaica, South Ozone Park)'
             ELSE                                                               '4. Ninguno toca Queens ni aeropuerto: error de digitación de la tarifa'
           END AS categoria,
           COUNT(*)                                   AS viajes,
           ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
           ROUND(AVG(total_amount), 2)                AS ticket_promedio,
           ROUND(AVG(trip_distance), 2)               AS distancia_promedio
    FROM jfk_rate_trips
    GROUP BY categoria
    ORDER BY categoria
"""))

# COMMAND ----------

# Pares origen → destino más frecuentes entre los que NO tocan JFK
display(spark.sql("""
    SELECT pu_borough, pu_zone, do_borough, do_zone, COUNT(*) AS viajes, ROUND(AVG(fare_amount), 2) AS tarifa_promedio
    FROM jfk_rate_trips
    WHERE NOT (pu_zone = 'JFK Airport' OR do_zone = 'JFK Airport')
    GROUP BY ALL
    ORDER BY viajes DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretación 3**
# MAGIC
# MAGIC - ✏️ El **__ %** de los viajes con tarifa JFK empieza o termina en la zona *JFK Airport*.
# MAGIC - La tarifa 2 es la **tarifa fija entre JFK y Manhattan** (en ambos sentidos). La **elige el conductor** en el
# MAGIC   taxímetro, mientras que la zona se asigna por GPS. El resto se explica por:
# MAGIC   1. **Ubicación no asignada (264/265).** El GPS no ubicó el pickup o el dropoff, pero el viaje sí fue al aeropuerto.
# MAGIC   2. **Zonas vecinas de Queens.** El pasajero se bajó o subió justo fuera del polígono *JFK Airport*, por ejemplo en
# MAGIC      hoteles del aeropuerto, estacionamientos o zonas de alquiler de autos. El viaje es de aeropuerto, pero la zona no.
# MAGIC   3. **Tarifa mal digitada.** Viajes a LaGuardia, Newark o dentro de Manhattan a los que el conductor les puso la
# MAGIC      tarifa JFK por error. Es un problema de calidad del dato en origen.
# MAGIC - Mira `tarifa_promedio` en el último cuadro. Si vale cerca de la tarifa fija de JFK, el viaje probablemente sí fue de
# MAGIC   aeropuerto y lo que falló es la zona. Si es muy distinta, lo que está mal es el código de tarifa.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pregunta 4. Propina promedio con tarjeta por `day_part` y fin de semana. ¿Cuándo se deja más propina?
# MAGIC
# MAGIC Sale de la vista `v_propinas_tarjeta`, que ya filtra `tip_is_recorded`. Como la vista viene por borough, se vuelve a
# MAGIC agregar **recalculando** el promedio con las sumas (`SUM(propinas_total) / SUM(viajes_tarjeta)`), **no** promediando
# MAGIC `propina_promedio`. Se muestran dos medidas:
# MAGIC
# MAGIC - **Propina promedio en $**, que depende de lo largo del viaje.
# MAGIC - **Propina como % de la tarifa** (`SUM(tip) / SUM(fare)`), que mide la "generosidad" sin el efecto de la distancia.

# COMMAND ----------

display(spark.sql(f"""
    SELECT franja_dia,
           CASE WHEN es_fin_de_semana THEN 'Fin de semana' ELSE 'Entre semana' END AS tipo_dia,
           SUM(viajes_tarjeta)                                         AS viajes_tarjeta,
           ROUND(SUM(propinas_total) / SUM(viajes_tarjeta), 2)         AS propina_promedio,
           ROUND(100 * SUM(propinas_total) / SUM(tarifa_total), 2)     AS propina_pct_tarifa
    FROM {T_DM_V_PROPINAS_TARJETA}
    GROUP BY franja_dia, es_fin_de_semana
    ORDER BY {DAY_PART_ORDER}, tipo_dia
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretación 4**
# MAGIC
# MAGIC - ✏️ La mayor propina promedio en dólares está en **__** (franja) **__** (entre semana o fin de semana): **$__**.
# MAGIC - ✏️ La mayor propina como % de la tarifa está en **__**: **__ %**.
# MAGIC - Qué esperar: la **madrugada** suele tener la propina promedio más alta **en dólares**, porque tiene más viajes
# MAGIC   largos (aeropuertos, regreso a otros boroughs). La **noche entre semana** suele tener el % más alto, con viajes de
# MAGIC   ocio y salidas de restaurantes. Las dos medidas pueden dar ganadores distintos, y esa diferencia es en sí misma un hallazgo.
# MAGIC - Esto solo es posible con la regla `tip_is_recorded`. Si se incluyera el efectivo, cada franja con más efectivo
# MAGIC   mostraría propinas artificialmente bajas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pregunta 5. Mezcla de formas de pago por mes: ¿qué participación tienen tarjeta, efectivo y Flex Fare?
# MAGIC
# MAGIC `payment_group` de `dim_payment_type` agrupa los códigos 3–6 (sin cargo, disputa, desconocido, anulado) como "Otros".

# COMMAND ----------

display(spark.sql(f"""
    SELECT d.year AS anio, d.month AS mes,
           COUNT(*)                                                                   AS viajes,
           ROUND(100 * COUNT_IF(p.payment_group = 'Tarjeta')   / COUNT(*), 2)         AS pct_tarjeta,
           ROUND(100 * COUNT_IF(p.payment_group = 'Efectivo')  / COUNT(*), 2)         AS pct_efectivo,
           ROUND(100 * COUNT_IF(p.payment_group = 'Flex Fare') / COUNT(*), 2)         AS pct_flex_fare,
           ROUND(100 * COUNT_IF(p.payment_group = 'Otros')     / COUNT(*), 2)         AS pct_otros,
           ROUND(100 * SUM(f.total_amount) FILTER (WHERE p.payment_group = 'Flex Fare')
                     / SUM(f.total_amount), 2)                                         AS pct_revenue_flex_fare
    FROM {FACT} f
    JOIN {D_DATE} d ON f.pickup_date_key  = d.date_key
    JOIN {D_PAY}  p ON f.payment_type_key = p.payment_type_key
    GROUP BY d.year, d.month
    ORDER BY anio, mes
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretación 5**
# MAGIC
# MAGIC - ✏️ La tarjeta pasa de **__ %** (inicio del periodo) a **__ %** (final). El efectivo pasa de **__ %** a **__ %**.
# MAGIC   Flex Fare aparece desde **__** y llega a **__ %**.
# MAGIC - Qué esperar: la tarjeta domina y el efectivo es la segunda forma de pago. **Flex Fare** (precio acordado de
# MAGIC   antemano desde una app) es marginal hasta diciembre de 2024 y crece con fuerza desde enero de 2025, cuando
# MAGIC   llega a cerca de un cuarto de los viajes, según el comentario de silver. Si se cargaran meses de 2025, su
# MAGIC   crecimiento le quitaría participación sobre todo a la tarjeta.
# MAGIC - **Implicaciones para Finanzas:**
# MAGIC   - Cada viaje que pasa a Flex Fare es un viaje **sin propina registrada** y **sin `congestion_surcharge` ni
# MAGIC     `airport_fee` publicados**. Las métricas de propinas y recargos de 2025 en adelante cubren una fracción menor del
# MAGIC     negocio, y hay que leerlas como "de los viajes donde se registra", no como del total.
# MAGIC   - Es justo la razón de dejar esos recargos en `NULL` y no en `0` (notebook 02).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pregunta 6. Peso de los recargos (`congestion_surcharge` + `airport_fee` + `cbd_congestion_fee`) sobre `total_amount`, por borough de pickup
# MAGIC
# MAGIC Se usa `v_revenue_mensual`: `recargos_total` y `revenue` son aditivos, así que se pueden re-agregar por borough. El
# MAGIC peso es `SUM(recargos) / SUM(revenue)`, nunca el promedio de porcentajes por viaje. Se abre por año porque
# MAGIC `cbd_congestion_fee` solo existe desde 2025.

# COMMAND ----------

display(spark.sql(f"""
    SELECT borough_pickup,
           SUM(viajes)                                                     AS viajes,
           SUM(revenue)                                                    AS revenue,
           SUM(recargo_congestion)                                         AS recargo_congestion,
           SUM(recargo_aeropuerto)                                         AS recargo_aeropuerto,
           SUM(recargo_cbd)                                                AS recargo_cbd,
           ROUND(100 * SUM(recargos_total) / SUM(revenue), 2)              AS pct_recargos_sobre_total
    FROM {T_DM_V_REVENUE_MENSUAL}
    GROUP BY borough_pickup
    ORDER BY pct_recargos_sobre_total DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT anio, borough_pickup,
           ROUND(100 * SUM(recargo_congestion) / SUM(revenue), 2)  AS pct_congestion,
           ROUND(100 * SUM(recargo_aeropuerto) / SUM(revenue), 2)  AS pct_aeropuerto,
           ROUND(100 * SUM(recargo_cbd)        / SUM(revenue), 2)  AS pct_cbd,
           ROUND(100 * SUM(recargos_total)     / SUM(revenue), 2)  AS pct_recargos_total
    FROM {T_DM_V_REVENUE_MENSUAL}
    GROUP BY anio, borough_pickup
    ORDER BY borough_pickup, anio
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretación 6**
# MAGIC
# MAGIC - ✏️ El borough con mayor peso de recargos es **__** (**__ %** del `total_amount`), y el de menor peso es **__** (**__ %**).
# MAGIC - Qué esperar:
# MAGIC   - **Manhattan** tiene el mayor peso del `congestion_surcharge`, porque se cobra en viajes que tocan Manhattan al
# MAGIC     sur de la calle 96. Desde 2025 se suma el `cbd_congestion_fee` (zona de cobro por congestión del MTA, al sur de
# MAGIC     la calle 60), así que el peso de Manhattan **sube desde enero de 2025**. En oct–dic 2024 `recargo_cbd` vale 0, como se decidió en el notebook 02.
# MAGIC   - **Queens** tiene peso por `airport_fee`, que solo se cobra en pickups en JFK y LaGuardia.
# MAGIC   - Los boroughs exteriores (Bronx, Staten Island) tienen el menor peso. Sus viajes rara vez entran a la zona de congestión.
# MAGIC - Matiz: como `congestion_surcharge` y `airport_fee` son `NULL` en Flex Fare, el peso de 2025 en adelante está
# MAGIC   **subestimado** en la proporción de Flex Fare: ese recargo existe, pero la TLC no lo desglosa.