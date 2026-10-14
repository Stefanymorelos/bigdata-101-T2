-- Top 20 pares origen→destino más frecuentes
-- Tabla administrada de Unity Catalog. Antes de ejecutar, selecciona en el
-- editor SQL el catálogo `20080_databricks` y tu schema (ej. camilo_soto).
--
-- Parámetro :fecha de tipo "Date Range" (crea :fecha.min y :fecha.max).
-- Se excluyen las zonas especiales 264 (Unknown) y 265 (Outside of NYC): silver
-- las conserva con texto, no con NULL, así que IS NOT NULL no las filtra.
SELECT
  pickup_zone,
  dropoff_zone,
  COUNT(*)                         AS trips,
  ROUND(AVG(trip_distance), 2)     AS avg_distance,
  ROUND(AVG(total_amount), 2)      AS avg_fare,
  ROUND(AVG(trip_duration_min), 2) AS avg_duration_min
FROM silver_yellow_trips_enriched
WHERE PULocationID NOT IN (264, 265)
  AND DOLocationID NOT IN (264, 265)
  AND (pickup_date BETWEEN :fecha.min AND :fecha.max OR :fecha IS NULL)
GROUP BY pickup_zone, dropoff_zone
ORDER BY trips DESC
LIMIT 20;
