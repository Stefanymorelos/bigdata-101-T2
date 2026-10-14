# 04 - Machine Learning

Entrenamiento, registro y despliegue de un modelo de regresión que predice la duración de un viaje en minutos a partir de información conocida al momento del pickup.

## Conceptos que se enseñan aquí

- **MLflow Tracking**: auto-logging de experimentos en Databricks
- **Unity Catalog Model Registry**: versionado y promoción de modelos
- **Batch inference** con el modelo registrado
- (Opcional) **Model Serving** como endpoint REST

## Orden de ejecución

1. `01_train_trip_duration.py` — Entrenar el modelo y loguearlo a MLflow
2. `02_register_model.py` — Registrar en Unity Catalog con aliases `@champion` / `@challenger`
3. `03_batch_inference.py` — Usar el modelo registrado para inferencia batch

## Cómputo

El pipeline usa Spark MLlib (`pyspark.ml`: `StringIndexer`, `GBTRegressor`) y `mlflow.spark`. Funciona en:

- **Cluster clásico estándar o dedicado** con Databricks Runtime 17.0 o superior. El runtime ya trae `mlflow-skinny`, no hay que instalar nada.
- **Serverless con environment version 4 o superior.** Cada notebook fija `environment_version = "5"` en su cabecera. Con entornos más viejos, `StringIndexer` falla con `Py4JSecurityException: ... is not whitelisted`.

En cómputo estándar y serverless, `mlflow.spark` exige un directorio temporal en un Volume de Unity Catalog para guardar o cargar modelos. Por eso los notebooks pasan `dfs_tmpdir=MLFLOW_DFS_TMPDIR`, definido en `00-setup/config.py`.

Con un 5% de muestra, el entrenamiento tarda unos 17 minutos en un cluster de 1 a 4 workers `Standard_D4ds_v5`.

## Modelo

Pipeline de **Spark MLlib** con `GBTRegressor` (gradient boosted trees), entrenado sobre la feature table de gold `T_GOLD_ML_FEATURES` (tabla `<CATALOG>.<SCHEMA>.gold_taxi_ml_features`). El objetivo es predecir `target_duration_min`.

Se registra en Unity Catalog como `<CATALOG>.<SCHEMA>.trip_duration_gbt`, valor de la constante `MODEL_NAME` en `00-setup/config.py`. Va en tu mismo schema: como eres su dueño ya tienes `CREATE MODEL`, y no hace falta un schema `_ml` aparte (los estudiantes no tienen `CREATE SCHEMA`).

Las predicciones batch se escriben en la tabla `T_ML_PREDICTIONS` (`gold_taxi_ml_predictions`). Como tanto las features como el modelo y las predicciones están en Unity Catalog, el linaje features → modelo → predicciones se ve completo en Catalog Explorer.

Baseline esperado: RMSE ~4-6 minutos, R² ~0.7-0.8 con 6 meses de datos.
