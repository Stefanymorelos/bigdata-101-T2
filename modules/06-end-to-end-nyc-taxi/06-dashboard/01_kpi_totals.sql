-- KPIs globales para counters del dashboard
-- Tabla administrada de Unity Catalog. Antes de ejecutar, selecciona en el
-- editor SQL el catálogo `20080_databricks` y tu schema (ej. camilo_soto).
--
-- Parámetro :fecha de tipo "Date Range" (crea :fecha.min y :fecha.max).
-- Si el usuario elige "All", :fecha llega como NULL y no se filtra.

SELECT
  COUNT(*)                                AS total_trips,
  ROUND(SUM(total_amount) / 1e6, 2)       AS total_revenue_millions,
  ROUND(AVG(total_amount), 2)             AS avg_fare,
  ROUND(AVG(trip_distance), 2)            AS avg_distance_mi,
  ROUND(AVG(trip_duration_min), 2)        AS avg_duration_min,
  ROUND(SUM(tip_amount) / 1e6, 2)         AS total_tips_millions,
  -- Solo tarjeta: en efectivo y Flex Fare la propina no se registra (queda en 0)
  ROUND(AVG(tip_rate) FILTER (WHERE payment_type = 1) * 100, 1) AS avg_tip_pct
FROM silver_yellow_trips_enriched
WHERE pickup_date BETWEEN :fecha.min AND :fecha.max OR :fecha IS NULL;
