# Pipeline End-to-End: NYC Taxi en Azure Databricks

Caso de estudio integrador que recorre todo el ciclo de vida de un pipeline de datos moderno sobre la plataforma Databricks en Azure, usando el dataset público de NYC Taxi & Limousine Commission (TLC).

## Objetivos de aprendizaje

Al terminar este módulo el estudiante es capaz de:

- Diseñar e implementar un pipeline Medallion (Bronze/Silver/Gold) sobre Delta Lake
- Ingestar archivos Parquet mensuales de forma incremental desde un Volume de Unity Catalog
- Aplicar limpieza, validación y enriquecimiento con DataFrame API
- Gobernar el lakehouse con Unity Catalog (catálogos, esquemas, volumes, permisos)
- Entrenar, registrar y servir un modelo de ML con MLflow
- Construir dashboards con Databricks SQL
- Orquestar todo con Databricks Workflows y Asset Bundles
- Agregar una capa de streaming sobre la misma arquitectura

## El dataset

**NYC TLC Yellow Taxi Trip Records** — ~1.5B filas acumuladas desde 2009, distribuidas como archivos Parquet mensuales.

- Origen oficial: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- CDN de descarga directa: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
- Tabla de zonas (lookup): `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
- Volumen mensual: 40-80 MB comprimido, 3-4M filas

Para la cátedra recomendamos arrancar con 6 meses (2024-01 a 2024-06) — suficiente para ver patrones estacionales sin explotar el cluster.

## Arquitectura

```
┌──────────────────┐   ┌─────────────────┐   ┌──────────────┐   ┌──────────────┐
│ CloudFront TLC   │──▶│ Landing (Volume │──▶│ Bronze       │──▶│ Silver       │
│ (Parquet mensual)│   │ UC, solo lectura│   │ Delta / path │   │ Clean + Join │
│  → lo baja el    │   │ del profesor)   │   │              │   │              │
│  profesor        │   │                 │   │              │   │              │
└──────────────────┘   └─────────────────┘   └──────────────┘   └──────────────┘
                                                                    │
                                           ┌────────────────────────┼───────────────────────┐
                                           ▼                        ▼                       ▼
                                    ┌──────────────┐         ┌──────────────┐        ┌──────────────┐
                                    │ Gold: KPIs   │         │ Gold: ML     │        │ Streaming    │
                                    │ Revenue      │         │ Features     │        │ (opcional)   │
                                    │ Heatmaps     │         │ Duration pred│        │              │
                                    └──────┬───────┘         └──────┬───────┘        └──────────────┘
                                           ▼                        ▼
                                    ┌──────────────┐         ┌──────────────┐
                                    │ Dashboard    │         │ Model Serving│
                                    │ Databricks   │         │ MLflow       │
                                    │ SQL          │         │ endpoint     │
                                    └──────────────┘         └──────────────┘
```

Todo gobernado por **Unity Catalog**, con esta convención:

- **Un catálogo compartido** por todo el curso (`20080_databricks`), creado por el profesor. Confirma el nombre del semestre con tu docente: cambia entre iteraciones y, como empieza por dígito, en SQL siempre va entre backticks.
- **Un schema por estudiante** dentro de ese catálogo: `<CATALOG>.<SCHEMA>`, donde `SCHEMA` es tu `nombre_apellido` en snake_case (por ejemplo `camilo_soto`).
- **Capas medallion como tablas administradas** de Unity Catalog en tu schema. La capa se distingue por prefijo (`bronze_yellow_trips`, `silver_yellow_trips`, `gold_taxi_ml_features`, `stream_bronze_yellow_trips`, ...), así que se consultan por nombre desde SQL, tienen linaje en Catalog Explorer y admiten `GRANT` por tabla.
- **Tu Volume `datos`**, el mismo de los módulos anteriores. Este módulo solo lo usa para archivos que no son tablas: checkpoints de Structured Streaming y el schema location de Auto Loader, bajo `_checkpoints/ny_taxi`.
- **Landing de solo lectura** en el schema `default` del catálogo compartido: `/Volumes/<CATALOG>/default/nytaxi_landing` (Parquet mensuales de la TLC) y `/Volumes/<CATALOG>/default/nytaxi_zones` (lookup de zonas). Los puebla el profesor.
- **El modelo de ML en tu mismo schema**: `<CATALOG>.<SCHEMA>.trip_duration_gbt`. No hay schema `_ml` aparte.

## Prerrequisitos

- Workspace de Azure Databricks con Unity Catalog habilitado
- El catálogo del curso **ya creado por el profesor** (el estudiante no crea catálogos)
- Permiso `USE CATALOG` sobre ese catálogo, y **tu schema ya creado por el profesor** con tu usuario como dueño (no tienes `CREATE SCHEMA`)
- Permiso de lectura (`READ VOLUME`) sobre los Volumes de landing `default.nytaxi_landing` y `default.nytaxi_zones`
- Cluster o SQL Warehouse con Runtime 14.3 LTS o superior (15.4 LTS recomendado)

> **Para el profesor/admin**: todos los pasos para configurar esto están en [`ADMIN_SETUP.md`](ADMIN_SETUP.md). Una vez hechos, los estudiantes solo deben poner su `SCHEMA` en `00-setup/config.py`.

## Estructura del módulo

```
00-setup/          Bootstrap: verifica tu schema, tu Volume datos y el landing
01-bronze/         Ingesta incremental mes a mes del landing a Bronze
02-silver/         Limpieza, validación, enriquecimiento con zonas
03-gold/           KPIs de negocio y feature tables para ML
04-ml/             Training, MLflow Registry, batch inference
05-streaming/      Versión streaming de Bronze→Silver
06-dashboard/      Queries SQL para dashboards de Databricks SQL
07-workflow/       Asset Bundle con el job orquestado
```

Cada carpeta tiene su propio README con la secuencia de ejecución sugerida.

## Secuencia recomendada (8 sesiones)

| Sesión | Tema | Carpetas |
|--------|------|----------|
| 1 | Setup + inspección del dataset | `00-setup/` |
| 2 | Bronze: ingesta mes a mes | `01-bronze/` |
| 3 | Silver: limpieza y joins | `02-silver/` |
| 4 | Gold: KPIs y optimización (OPTIMIZE, ZORDER) | `03-gold/` |
| 5 | Dashboard en Databricks SQL | `06-dashboard/` |
| 6 | ML end-to-end con MLflow | `04-ml/` |
| 7 | Streaming con la misma arquitectura | `05-streaming/` |
| 8 | Orquestación con Workflows y Asset Bundles | `07-workflow/` |

## Ejecución rápida

```bash
# Desde Databricks CLI con Asset Bundles
databricks bundle deploy --target dev
databricks bundle run nyctaxi_pipeline --target dev
```

O notebook por notebook desde el workspace siguiendo el orden de las carpetas.

## Convenciones

- Todos los estudiantes comparten el catálogo del curso y cada uno trabaja en **su propio schema** dentro de él
- Lo único que se edita es `SCHEMA` en `00-setup/config.py` (tu `nombre_apellido` en snake_case)
- Los datos se escriben como **tablas administradas** con `saveAsTable` / `toTable` y se leen con `spark.table` / `readStream.table`; nunca con `save(path)` / `load(path)`
- Todos los nombres de tabla son constantes `T_*` de `config.py`, derivadas de `CATALOG` y `SCHEMA`: cambiar `SCHEMA` reubica todas las tablas
- Los nombres llevan `yellow_trips` / `taxi_` porque el módulo 04 escribe en el mismo schema (`bronze_travel_times`, ...) y así no chocan
- El modelo se registra en Unity Catalog como `<CATALOG>.<SCHEMA>.trip_duration_gbt` (constante `MODEL_NAME`)
- Todo el código en Python (PySpark) con SQL embebido donde sea más claro
- Variables de configuración centralizadas en `00-setup/config.py`; los notebooks la importan con `%run`
