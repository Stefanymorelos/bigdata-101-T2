-- Prueba de permisos (Parte 3.3) — la corre TU COMPAÑERO, no tú.
-- Ejecutar en un SQL warehouse (SQL Editor) o en un notebook serverless.
-- Reemplaza <catalogo> por el catálogo del curso y <schema_duenio> por el schema
-- de quien otorgó los permisos (su nombre_apellido).

-- ✓ 1. Debe funcionar: vistas de consumo
SELECT * FROM `<catalogo>`.`<schema_duenio>`.gold_taxi_dm_v_revenue_mensual
ORDER BY anio DESC, mes DESC, revenue DESC
LIMIT 10;

SELECT * FROM `<catalogo>`.`<schema_duenio>`.gold_taxi_dm_v_propinas_tarjeta
LIMIT 10;

-- ✗ 2. Debe FALLAR (PERMISSION_DENIED / INSUFFICIENT_PERMISSIONS): tabla de hechos
SELECT * FROM `<catalogo>`.`<schema_duenio>`.gold_taxi_dm_fact_trips LIMIT 10;

-- ✗ 3. Debe FALLAR: silver
SELECT * FROM `<catalogo>`.`<schema_duenio>`.silver_yellow_trips_enriched LIMIT 10;

-- Copia el mensaje de error completo de 2 y 3 y pásaselo al dueño para su entrega.
