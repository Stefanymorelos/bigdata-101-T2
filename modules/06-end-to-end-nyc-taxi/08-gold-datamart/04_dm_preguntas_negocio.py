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
# MAGIC - **Manhattan concentra el negocio.** Representa unos $418,8 M de los $522,7 M del periodo (≈ 80 % del revenue)
# MAGIC   y 18,05 M de los 19,9 M de viajes. Casi todo cae en `service_zone = Yellow Zone`, el mercado natural del taxi amarillo.
# MAGIC - **Los aeropuertos pesan mucho más en dinero que en viajes.** Los viajes que **empiezan** en un aeropuerto son
# MAGIC   1.147.495 (≈ 5,8 % de los viajes) y generan **$80,9 M, el 15,48 % del revenue**. Son viajes largos, con tarifa fija
# MAGIC   a JFK, peajes y `airport_fee`.
# MAGIC   - JFK: 592.496 viajes, $43,9 M (**8,39 %**).
# MAGIC   - LaGuardia: 554.939 viajes, $37,0 M (**7,09 %**).
# MAGIC   - Newark: solo 60 viajes ($2.285). Está en Nueva Jersey y los taxis amarillos de NYC prácticamente no recogen allí.
# MAGIC - La tabla mensual muestra además la participación de los viajes que **tocan** un aeropuerto (empiezan o terminan),
# MAGIC   que es mayor porque suma los viajes **hacia** el aeropuerto.

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
# MAGIC - **Ticket promedio hacia aeropuertos: $71,91 en festivos vs. $73,43 en días normales**, un 2,1 % menor. La
# MAGIC   distancia promedio es casi igual (12,32 vs. 12,06 millas), así que la diferencia no viene de viajes más cortos.
# MAGIC   Viene de que en festivo hay menos tráfico (menos tiempo cobrado en el taxímetro) y no aplica el recargo de hora pico.
# MAGIC   La demanda también baja: 1.646 viajes por día en festivo vs. 1.840 en día normal.
# MAGIC - **Por aeropuerto el patrón se repite en JFK y LaGuardia.** JFK: $82,52 entre semana, $81,51 fin de semana y $80,06
# MAGIC   en festivo. LaGuardia: $68,97, $66,19 y $66,58. El festivo se comporta como un fin de semana: el efecto es "día no
# MAGIC   laboral", no el festivo en sí. Newark es la excepción ($89,87 en festivo), pero con solo 89 viajes no es concluyente.
# MAGIC - **No todos los festivos son iguales.** Veterans Day (2.530 viajes/día) y Columbus Day (2.311) mueven más viajes al
# MAGIC   aeropuerto que un día normal, porque mucha gente trabaja y viaja esos días. En cambio Thanksgiving (897) y Navidad (886)
# MAGIC   tienen la mitad de la demanda: los viajes se hacen **antes** del festivo, no el mismo día.

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
# MAGIC - **El 96,44 % de los viajes con tarifa JFK** (393.964 de 408.524) empieza o termina en la zona *JFK Airport*.
# MAGIC - La tarifa 2 es la **tarifa fija entre JFK y Manhattan** (en ambos sentidos). La **elige el conductor** en el
# MAGIC   taxímetro, mientras que la zona se asigna por GPS. El 3,56 % restante se explica por:
# MAGIC   1. **Zonas vecinas de Queens (2,24 %, 9.140 viajes).** El viaje es de aeropuerto, pero la zona asignada no es
# MAGIC      *JFK Airport*. El par más frecuente es *Baisley Park* (zona contigua a JFK) → Manhattan, y todos los pares
# MAGIC      del top tienen `fare_amount` = **$70**, que es exactamente la tarifa fija de JFK. El cobro fue correcto: falló
# MAGIC      la asignación de zona.
# MAGIC   2. **Error de digitación de la tarifa (0,69 %, 2.816 viajes).** No tocan Queens ni ningún aeropuerto, con una
# MAGIC      distancia promedio de solo 2,68 millas, pero se les cobró la tarifa de JFK (ticket promedio $84,69). Son viajes
# MAGIC      dentro de Manhattan con el código de tarifa mal puesto, lo que también implica un **sobrecobro** al pasajero.
# MAGIC   3. **Otro aeropuerto (0,35 %, 1.438 viajes).** Viajes desde o hacia LaGuardia con la tarifa de JFK.
# MAGIC   4. **Ubicación no asignada (0,29 %, 1.166 viajes).** Zonas 264/265: el GPS no ubicó el viaje, pero la tarifa y la
# MAGIC      distancia (16,8 millas) indican que sí fue al aeropuerto.

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
# MAGIC - **En dólares, la mayor propina está en la madrugada entre semana: $4,31 por viaje.** Son viajes más largos
# MAGIC   (aeropuerto, regreso a casa desde otros boroughs). La menor está en la madrugada de fin de semana ($3,39), con viajes
# MAGIC   cortos de salida nocturna.
# MAGIC - **Como % de la tarifa, la mayor propina está en la noche entre semana: 24,00 %**, seguida por la tarde entre semana
# MAGIC   (23,39 %). Es la franja de cenas y salidas después del trabajo.
# MAGIC - **Entre semana se deja más propina que el fin de semana** en casi todas las franjas, en dólares y en %. La única
# MAGIC   excepción es la mañana, donde el fin de semana deja un % un poco mayor (23,08 % vs. 22,84 %).
# MAGIC - Las dos medidas dan ganadores distintos, y eso confirma que el promedio en dólares depende sobre todo de lo largo del
# MAGIC   viaje, no de la generosidad del pasajero.
# MAGIC - Este análisis es posible gracias a la regla `tip_is_recorded`: si se incluyeran efectivo y Flex Fare (propina en 0),
# MAGIC   las franjas con más efectivo mostrarían propinas artificialmente bajas.

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
# MAGIC | Mes | Tarjeta | Efectivo | Flex Fare | Otros |
# MAGIC |---|---|---|---|---|
# MAGIC | oct-2024 | 77,54 % | 12,28 % | 9,74 % | 0,44 % |
# MAGIC | dic-2024 | 77,83 % | 13,05 % | 8,64 % | 0,48 % |
# MAGIC | ene-2025 | 75,12 % | 11,09 % | 13,36 % | 0,43 % |
# MAGIC | mar-2025 | 69,98 % | 9,71 % | 19,89 % | 0,42 % |
# MAGIC
# MAGIC - **La tarjeta domina**, pero cae de 77,5 % a 70,0 % en seis meses. El efectivo baja de 12,3 % a 9,7 %.
# MAGIC - **Flex Fare se duplica**: estable alrededor de 9–10 % en el último trimestre de 2024, salta a 13,4 % en enero de
# MAGIC   2025 y llega a **19,9 %** en febrero y marzo. Su participación en revenue sigue la misma tendencia (8,7 % → 19,2 %).
# MAGIC   Le quita participación tanto a la tarjeta como al efectivo.
# MAGIC - **Implicaciones para Finanzas:**
# MAGIC   - Cada viaje que pasa a Flex Fare es un viaje **sin propina registrada** y **sin `congestion_surcharge` ni
# MAGIC     `airport_fee` publicados**. Las métricas de propinas y recargos de 2025 cubren una fracción menor del negocio y
# MAGIC     deben leerse como "de los viajes donde se registra", no como del total.
# MAGIC   - Es justo la razón de dejar esos recargos en `NULL` y no en `0` (notebook 02): con 0, el recargo promedio caería
# MAGIC     artificialmente a medida que crece Flex Fare.

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
# MAGIC - **Manhattan es el borough con mayor peso de recargos: 10,54 % del `total_amount`.** Casi todo es
# MAGIC   `congestion_surcharge` ($39,0 M), que se cobra en viajes que tocan Manhattan al sur de la calle 96, más el
# MAGIC   `cbd_congestion_fee` ($5,1 M).
# MAGIC - **El efecto del cobro CBD se ve al abrir por año:** en Manhattan los recargos pasan de 9,47 % (oct–dic 2024) a
# MAGIC   **11,69 %** (ene–mar 2025), y la diferencia es casi toda el `cbd_congestion_fee` (2,55 % en 2025, 0 % en 2024, como
# MAGIC   se decidió en el notebook 02). El peso de la congestión en sí se mantiene estable (9,47 % → 9,14 %).
# MAGIC - **Queens (4,44 %)** tiene un peso repartido entre congestión (≈ 2 %) y `airport_fee` (≈ 2,2 %), que solo se cobra
# MAGIC   en pickups en JFK y LaGuardia. En 2025 sube a 4,60 % por el CBD.
# MAGIC - Los boroughs exteriores tienen el menor peso: **Brooklyn 0,96 %, Bronx 0,32 % y Staten Island 0,28 %**. Sus viajes
# MAGIC   rara vez entran a la zona de congestión. *Unknown* (8,29 %) se parece a Manhattan, lo que sugiere que buena parte de
# MAGIC   esos viajes sin zona asignada son en realidad de Manhattan.
# MAGIC - Matiz: como `congestion_surcharge` y `airport_fee` son `NULL` en Flex Fare, el peso de 2025 está **subestimado** en
# MAGIC   la proporción de Flex Fare (≈ 20 % de los viajes en feb–mar 2025): ese recargo existe, pero la TLC no lo desglosa.
