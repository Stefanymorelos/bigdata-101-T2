# 05 - Streaming (opcional)

Versión streaming de Bronze → Silver usando la misma arquitectura Medallion. Sirve para mostrar que Delta Lake y Auto Loader soportan batch y streaming con el mismo código — uno de los diferenciadores del Lakehouse.

## Conceptos que se enseñan aquí

- Structured Streaming sobre Delta
- `foreachBatch` para escrituras complejas
- Watermarks y manejo de datos tardíos
- `readStream` sobre una tabla Delta (CDC implícito)

## Orden de ejecución

1. `01_streaming_bronze.py` — Stream de archivos desde el landing (`LANDING_TRIPS_PATH`) hacia la tabla `T_STREAM_BRONZE` (`writeStream.toTable`)
2. `02_streaming_silver.py` — Stream sobre esa tabla (`readStream.table`) que alimenta `T_STREAM_SILVER`

## Tablas y checkpoints

Las capas de streaming son tablas administradas de Unity Catalog, separadas de las batch para no mezclar estados. Los checkpoints sí son archivos, así que van en el Volume:

| Constante de `config.py` | Dónde |
|--------------------------|-------|
| `T_STREAM_BRONZE` | Tabla `<CATALOG>.<SCHEMA>.stream_bronze_yellow_trips` |
| `T_STREAM_SILVER` | Tabla `<CATALOG>.<SCHEMA>.stream_silver_yellow_trips` |
| `CHECKPOINT_BASE` | `/Volumes/<CATALOG>/<SCHEMA>/datos/_checkpoints/ny_taxi` |

Cada query cuelga su checkpoint de un subdirectorio de `CHECKPOINT_BASE`. Si quieres reprocesar desde cero, borra el checkpoint **y** la tabla de salida (`DROP TABLE`): el checkpoint es lo que recuerda qué archivos ya se leyeron, y si solo borras uno de los dos el stream queda desincronizado.

## Nota

Esta sección es **demostrativa**. Para correr de verdad necesitas:
- Un cluster siempre encendido (costoso)
- Archivos llegando periódicamente al landing. El landing es de solo lectura y lo puebla el profesor, así que en el curso no hay llegadas nuevas mientras corres el stream; no existe un simulador de llegadas en este módulo.

Para fines del curso, correr los notebooks por unos minutos para ver la UI de streaming es suficiente.
