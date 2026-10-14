# 00 - Setup

Bootstrap del pipeline: verifica que tu **schema** (creado por el profesor) existe y puedes crear tablas en él, asegura tu **Volume `datos`**, y comprueba que puedes leer el landing con los archivos de NYC Taxi.

## Prerrequisitos

El profesor ya dejó listas dos cosas en el workspace, y ninguna la crea el estudiante:

1. El **catálogo compartido** del curso (`20080_databricks`; confirma el nombre del semestre con tu docente).
2. El **landing de solo lectura**, dos Volumes UC en el schema `default` de ese catálogo: `default.nytaxi_landing` (Parquet mensuales de la TLC) y `default.nytaxi_zones` (lookup de zonas).

Lo que necesitas tener a tu nombre: `USE CATALOG` sobre el catálogo, tu schema `nombre_apellido` con tu usuario como dueño (lo crea el profesor; tú no tienes `CREATE SCHEMA`), y `READ VOLUME` sobre los dos Volumes del landing. Si te faltan, pídeselos al profesor (ver `ADMIN_SETUP.md`).

## Orden de ejecución

1. `config.py` — No se ejecuta directamente, se importa con `%run`. Centraliza catálogo, schema, Volume y todas las rutas. **Edita `SCHEMA` antes que nada.**
2. `01_create_catalog.py` — Activa el catálogo compartido, verifica tu schema (y que puedes crear tablas en él) y crea tu Volume `datos` si falta. No crea catálogos ni schemas: si alguno no existe o no tienes permiso, falla a propósito con un mensaje para el profesor.
3. `02_explore_landing.py` — Inspecciona el esquema y el contenido del landing para entender el dataset.

`_admin_download_landing.py` es **solo del profesor**: puebla el landing compartido. Los estudiantes no lo corren.

## Qué vas a tener después de correr esto

- Tu schema de trabajo `<CATALOG>.<SCHEMA>` verificado. Ahí van todas tus tablas y también el modelo de ML
- Tu Volume `<CATALOG>.<SCHEMA>.datos`; este módulo solo lo usa para checkpoints (`CHECKPOINT_BASE`)
- Los nombres de tabla ya resueltos e impresos: `T_BRONZE_TRIPS`, `T_BRONZE_ZONES`, `T_SILVER_TRIPS`, `T_SILVER_TRIPS_ENRICHED`, `T_GOLD_ML_FEATURES`, `T_STREAM_BRONZE`, `T_STREAM_SILVER`, entre otros. Las tablas en sí no existen todavía: cada notebook crea la suya la primera vez que escribe
- Confirmación de que el landing (`LANDING_TRIPS_PATH`, `LANDING_ZONES_PATH`) es legible

Los datos todavía no existen: las capas se llenan en `01-bronze/` y siguientes.

## Variables que debes ajustar

En el bloque editable de `config.py`, **una sola**:

- `SCHEMA`: tu `nombre_apellido` en snake_case (por ejemplo `camilo_soto`). Define tu schema y, con él, dónde quedan todas tus tablas.

`CATALOG` lo crea el profesor (confírmalo con tu docente si cambió de semestre) y `VOLUME` es siempre `datos`.

> Los meses que se ingestan **no** se configuran aquí: `MONTHS_TO_INGEST` vive en `01-bronze/01_bronze_trips.py`.
