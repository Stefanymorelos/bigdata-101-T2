# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: demanda por hora y zona
# MAGIC
# MAGIC Demanda por zona, fecha y hora, para `06-dashboard/04_heatmap_demand.sql`.
# MAGIC Lee `T_SILVER_TRIPS_ENRICHED` y escribe la tabla administrada `T_GOLD_HOURLY_DEMAND`.
# MAGIC
# MAGIC **Grano: zona × fecha × hora.** Se guarda `pickup_date` (y no solo el día de
# MAGIC la semana) para que el heatmap pueda:
# MAGIC - filtrar por rango de fechas, y
# MAGIC - mostrar viajes **promedio** por día de semana y hora, dividiendo por cuántas
# MAGIC   veces aparece cada día de la semana en el rango. Con sumas, un rango con dos
# MAGIC   lunes y un martes inflaría los lunes.
# MAGIC
# MAGIC Igual que en revenue por zona, se excluyen las zonas especiales 264
# MAGIC (`Unknown`) y 265 (`Outside of NYC`).

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

UNKNOWN_ZONE_IDS = "264, 265"

spark.sql(f"""
    CREATE OR REPLACE TABLE {T_GOLD_HOURLY_DEMAND} AS
    SELECT pickup_date, pickup_borough, pickup_zone, pickup_hour, pickup_dayofweek,
      is_weekend, is_rush_hour,
      COUNT(*)                      AS trips,
      ROUND(AVG(total_amount), 2)   AS avg_fare,
      ROUND(AVG(avg_speed_mph), 2)  AS avg_speed_mph
    FROM {T_SILVER_TRIPS_ENRICHED}
    WHERE PULocationID NOT IN ({UNKNOWN_ZONE_IDS})
    GROUP BY pickup_date, pickup_borough, pickup_zone, pickup_hour, pickup_dayofweek, is_weekend, is_rush_hour
""")

# El dashboard filtra por fecha y borough
spark.sql(f"OPTIMIZE {T_GOLD_HOURLY_DEMAND} ZORDER BY (pickup_date, pickup_borough)")
print(f"✓ {T_GOLD_HOURLY_DEMAND}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zonas con más demanda promedio en hora pico
# MAGIC
# MAGIC El promedio divide por los días distintos del periodo, no por los días en
# MAGIC que la zona tuvo viajes: un día sin viajes en una zona cuenta como 0.

# COMMAND ----------

display(spark.sql(f"""
    WITH dias AS (SELECT COUNT(DISTINCT pickup_date) AS n FROM {T_GOLD_HOURLY_DEMAND})
    SELECT pickup_zone, pickup_hour,
      ROUND(SUM(trips) / MAX(dias.n), 1) AS avg_trips_por_dia
    FROM {T_GOLD_HOURLY_DEMAND}, dias
    WHERE is_rush_hour = true
    GROUP BY pickup_zone, pickup_hour
    ORDER BY avg_trips_por_dia DESC LIMIT 10
"""))
