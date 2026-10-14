-- Distribución del porcentaje de propina (bucketizado)
-- Tabla administrada de Unity Catalog. Antes de ejecutar, selecciona en el
-- editor SQL el catálogo `20080_databricks` y tu schema (ej. camilo_soto).
--
-- Parámetro :fecha de tipo "Date Range" (crea :fecha.min y :fecha.max).
SELECT
  CASE
    WHEN tip_rate = 0              THEN '0%'
    WHEN tip_rate < 0.10           THEN '< 10%'
    WHEN tip_rate < 0.15           THEN '10-15%'
    WHEN tip_rate < 0.20           THEN '15-20%'
    WHEN tip_rate < 0.25           THEN '20-25%'
    ELSE '25%+'
  END AS tip_bucket,
  COUNT(*) AS trips,
  ROUND(AVG(fare_amount), 2) AS avg_fare
FROM silver_yellow_trips_enriched
WHERE payment_type = 1   -- Solo tarjeta (cash tips no se registran)
  AND (pickup_date BETWEEN :fecha.min AND :fecha.max OR :fecha IS NULL)
GROUP BY tip_bucket
ORDER BY
  CASE tip_bucket
    WHEN '0%'      THEN 1 WHEN '< 10%'  THEN 2 WHEN '10-15%' THEN 3
    WHEN '15-20%'  THEN 4 WHEN '20-25%' THEN 5 ELSE 6
  END;
