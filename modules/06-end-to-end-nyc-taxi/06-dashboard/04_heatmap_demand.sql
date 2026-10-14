-- Heatmap: viajes promedio por hora × día de semana
-- Tabla administrada de Unity Catalog. Antes de ejecutar, selecciona en el
-- editor SQL el catálogo `20080_databricks` y tu schema (ej. camilo_soto).
--
-- Parámetros:
--   :fecha   tipo "Date Range" (crea :fecha.min y :fecha.max); NULL = "All"
--   :borough tipo String; NULL = "All"
--
-- Muestra el PROMEDIO por día, no la suma: cada día de la semana se divide
-- por cuántas veces aparece en el rango. El denominador se calcula sin filtrar
-- por borough, así un día sin viajes en el borough cuenta como 0.
WITH rango AS (
  SELECT *
  FROM gold_taxi_hourly_demand
  WHERE pickup_date BETWEEN :fecha.min AND :fecha.max OR :fecha IS NULL
),
dias AS (
  SELECT pickup_dayofweek, COUNT(DISTINCT pickup_date) AS n_dias
  FROM rango
  GROUP BY pickup_dayofweek
)
SELECT
  r.pickup_dayofweek,
  CASE r.pickup_dayofweek
    WHEN 1 THEN 'Dom' WHEN 2 THEN 'Lun' WHEN 3 THEN 'Mar'
    WHEN 4 THEN 'Mié' WHEN 5 THEN 'Jue' WHEN 6 THEN 'Vie'
    WHEN 7 THEN 'Sáb'
  END AS day_name,
  r.pickup_hour,
  ROUND(SUM(r.trips) / MAX(d.n_dias), 1) AS avg_trips
FROM rango r
JOIN dias d ON r.pickup_dayofweek = d.pickup_dayofweek
WHERE r.pickup_borough = COALESCE(:borough, r.pickup_borough)
GROUP BY r.pickup_dayofweek, r.pickup_hour
ORDER BY r.pickup_dayofweek, r.pickup_hour;
