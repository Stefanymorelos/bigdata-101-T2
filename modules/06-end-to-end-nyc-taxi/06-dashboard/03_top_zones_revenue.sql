-- Top 20 zonas de pickup por revenue total
-- Tabla administrada de Unity Catalog. Antes de ejecutar, selecciona en el
-- editor SQL el catálogo `20080_databricks` y tu schema (ej. camilo_soto).
SELECT
  pickup_zone,
  pickup_borough,
  total_trips,
  total_revenue,
  avg_fare,
  avg_tip_pct
FROM gold_taxi_revenue_by_zone
WHERE pickup_borough = COALESCE(:borough, pickup_borough)
ORDER BY total_revenue DESC
LIMIT 20;
