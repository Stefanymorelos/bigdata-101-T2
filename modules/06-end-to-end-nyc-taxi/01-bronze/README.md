# 01 - Bronze

Ingesta incremental del landing a Delta en la capa Bronze. El principio: los datos entran tal cual, en Delta, sin transformaciones de negocio.

## Conceptos que se enseñan aquí

- **Ingesta idempotente mes a mes**: se lleva registro de qué archivos ya se procesaron consultando `_source_file` en la tabla de bronze, así que volver a correr el notebook no duplica filas
- **Unificación de esquemas inconsistentes**: los Parquet de la TLC cambian tipos y nombres de columna entre meses; `read_yellow_trips_month` de `config.py` detecta la variante y la normaliza
- **Modo `append`** sobre una tabla administrada de Unity Catalog (`saveAsTable`)
- **Metadatos de ingesta**: columnas técnicas (`_ingested_at`, `_source_file`) para trazabilidad
- **Tablas estáticas**: cuando la fuente es un archivo pequeño (zonas) se lee y se sobreescribe completo

## Orden de ejecución

1. `01_bronze_trips.py` — Ingesta de los Parquet mensuales del landing. Ajusta `MONTHS_TO_INGEST` (se define en este mismo notebook) para elegir los meses.
2. `02_bronze_zones.py` — Tabla estática de zonas desde el lookup CSV del landing.

## Resultado esperado

Tablas administradas de Unity Catalog en tu schema, con los nombres que define `00-setup/config.py`:

| Constante | Tabla | Contenido |
|-----------|-------|-----------|
| `T_BRONZE_TRIPS` | `<CATALOG>.<SCHEMA>.bronze_yellow_trips` | Viajes en modo append, ~3-4M filas por mes ingestado |
| `T_BRONZE_ZONES` | `<CATALOG>.<SCHEMA>.bronze_taxi_zones` | 265 zonas de taxi |

Se consultan por nombre desde SQL, por ejemplo ``SELECT COUNT(*) FROM `20080_databricks`.camilo_soto.bronze_yellow_trips``, y aparecen en Catalog Explorer con historial y linaje.
