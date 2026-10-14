-- Prueba de permisos (Parte 3.3)
-- Ejecutada por Santiago Mendoza (santiago.mendozam@upb.edu.co) desde su cuenta,
-- en el SQL Editor con el Serverless Starter Warehouse.
-- Permisos otorgados: USE SCHEMA sobre stefany_morelos + SELECT solo sobre las vistas del mart.

-- ✓ 1. Se espera que funcione: vistas de consumo
SELECT * FROM `20080_databricks`.`stefany_morelos`.gold_taxi_dm_v_revenue_mensual
ORDER BY anio DESC, mes DESC, revenue DESC
LIMIT 10;

SELECT * FROM `20080_databricks`.`stefany_morelos`.gold_taxi_dm_v_propinas_tarjeta
LIMIT 10;

-- ✗ 2. Se espera error de permisos: tabla de hechos
SELECT * FROM `20080_databricks`.`stefany_morelos`.gold_taxi_dm_fact_trips LIMIT 10;

-- ✗ 3. Se espera error de permisos: silver
SELECT * FROM `20080_databricks`.`stefany_morelos`.silver_yellow_trips_enriched LIMIT 10;
