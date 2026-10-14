-- Serie temporal diaria: trips y revenue
-- Tabla administrada de Unity Catalog. Antes de ejecutar, selecciona en el
-- editor SQL el catálogo `20080_databricks` y tu schema (ej. camilo_soto).
--
-- Parámetro :fecha de tipo "Date Range" (crea :fecha.min y :fecha.max).
-- Ojo: del 2023-09-21 al 2023-09-24 casi no hay viajes. El hueco viene del
-- parquet de la TLC, no del pipeline.
SELECT
  pickup_date,
  total_trips,
  total_revenue,
  avg_fare
FROM gold_taxi_daily_metrics
WHERE pickup_date BETWEEN :fecha.min AND :fecha.max OR :fecha IS NULL
ORDER BY pickup_date;
