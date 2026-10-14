# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: revenue por zona
# MAGIC
# MAGIC KPIs de negocio por zona de pickup, para `06-dashboard/03_top_zones_revenue.sql`.
# MAGIC Lee `T_SILVER_TRIPS_ENRICHED` y escribe la tabla administrada `T_GOLD_REVENUE_BY_ZONE`.
# MAGIC
# MAGIC Se excluyen las zonas especiales 264 (`Unknown`) y 265 (`Outside of NYC`):
# MAGIC silver las conserva porque son viajes reales, pero no son una zona de NYC
# MAGIC con la que tenga sentido comparar revenue.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

UNKNOWN_ZONE_IDS = "264, 265"

spark.sql(f"""
    CREATE OR REPLACE TABLE {T_GOLD_REVENUE_BY_ZONE} AS
    SELECT pickup_borough, pickup_zone, PULocationID,
      COUNT(*)                          AS total_trips,
      ROUND(SUM(total_amount), 2)       AS total_revenue,
      ROUND(AVG(total_amount), 2)       AS avg_fare,
      ROUND(SUM(tip_amount), 2)         AS total_tips,
      -- Solo tarjeta: en efectivo y Flex Fare la propina no se registra (queda en 0)
      ROUND(AVG(tip_rate) FILTER (WHERE payment_type = 1) * 100, 2) AS avg_tip_pct,
      ROUND(AVG(trip_distance), 2)      AS avg_distance_mi,
      ROUND(AVG(trip_duration_min), 2)  AS avg_duration_min
    FROM {T_SILVER_TRIPS_ENRICHED}
    WHERE PULocationID NOT IN ({UNKNOWN_ZONE_IDS})
    GROUP BY pickup_borough, pickup_zone, PULocationID
""")

spark.sql(f"OPTIMIZE {T_GOLD_REVENUE_BY_ZONE} ZORDER BY (pickup_borough)")
print(f"✓ {T_GOLD_REVENUE_BY_ZONE}")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {T_GOLD_REVENUE_BY_ZONE} ORDER BY total_revenue DESC LIMIT 20"))
