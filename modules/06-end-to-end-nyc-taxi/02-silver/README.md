# 02 - Silver

Limpieza, validación y enriquecimiento. Los datos de Bronze pasan por reglas de calidad y se unen con la tabla de zonas para tener nombres legibles.

## Conceptos que se enseñan aquí

- Reglas de calidad con `filter` + tabla de **rechazos** para no perder datos
- **Schema enforcement**: tipos estrictos en Silver
- **Broadcast join** con una tabla pequeña (zonas) → join eficiente sin shuffle
- Derivar columnas analíticas (duración, hora del día, día de semana)
- Idempotencia con `replaceWhere` sobre tablas particionadas — reprocesable desde Bronze
- Detección de tablas existentes con `spark.catalog.tableExists` en lugar de atrapar excepciones

## Tablas generadas

Tablas administradas de Unity Catalog en `<CATALOG>.<SCHEMA>`:

| Constante | Tabla | Contenido |
|-----------|-------|-----------|
| `T_SILVER_TRIPS` | `silver_yellow_trips` | Viajes válidos, particionados por `pickup_year` / `pickup_month` |
| `T_SILVER_REJECTED` | `silver_yellow_trips_rejected` | Viajes rechazados con `rejection_reasons` |
| `T_SILVER_TRIPS_ENRICHED` | `silver_yellow_trips_enriched` | Viajes con zonas y features, particionados por `pickup_date` |
| `T_SILVER_QUALITY_METRICS` | `silver_yellow_trips_quality` | Una fila por ejecución con conteos y fallos por regla |

## Orden de ejecución

1. `01_silver_trips_clean.py` — Aplicar reglas de limpieza y separar válidos/rechazados
2. `02_silver_trips_enriched.py` — Enriquecer con zonas y features temporales
3. `03_data_quality_report.py` — Reporte de calidad y métricas de rechazo

## Reglas de limpieza aplicadas

| # | Regla | Por qué |
|---|-------|---------|
| 1 | `trip_distance > 0` | Trip con 0 millas es un error del taxímetro |
| 2 | `fare_amount >= 0` | Tarifas negativas son reembolsos, no viajes |
| 3 | `total_amount > 0` | Lo mismo |
| 4 | `passenger_count BETWEEN 1 AND 8` | Taxis caben hasta 6-8 pasajeros; 0 o null es error |
| 5 | `tpep_dropoff_datetime > tpep_pickup_datetime` | El dropoff debe ser posterior al pickup |
| 6 | `duration BETWEEN 1 minuto Y 6 horas` | Viajes fuera de ese rango son anómalos |
| 7 | `PULocationID` y `DOLocationID` entre 1 y 265 | IDs fuera del lookup se descartan. Se conservan 264 (`Unknown`) y 265 (`Outside of NYC`): son viajes reales sin zona asignable |
| 8 | `payment_type` entre 0 y 6 | `0` es *Flex Fare trip* en el diccionario de la TLC; ~24% de los viajes de 2025 |
