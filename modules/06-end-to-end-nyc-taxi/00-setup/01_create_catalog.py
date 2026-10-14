# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: tu schema y tu Volume en Unity Catalog
# MAGIC
# MAGIC El **catálogo y tu schema los crea el profesor**: el catálogo es compartido
# MAGIC por todo el curso y tu schema ya existe a tu nombre. Este notebook no crea
# MAGIC ninguno de los dos; verifica que tu espacio está listo:
# MAGIC
# MAGIC 1. Que tu schema `<CATALOG>.<SCHEMA>` existe y puedes crear tablas en él.
# MAGIC    Ahí van todas las capas (tablas administradas, las crea cada notebook la
# MAGIC    primera vez que escribe) y también el modelo de ML.
# MAGIC 2. Tu Volume `datos` (lo crea si falta), que aquí solo se usa para
# MAGIC    checkpoints de streaming.
# MAGIC 3. Que puedes leer el landing compartido.
# MAGIC
# MAGIC ## Antes de correr
# MAGIC Abre `config.py` y en el **bloque editable** cambia `SCHEMA` por tu
# MAGIC `nombre_apellido` en snake_case. Es lo único que debes editar: `VOLUME` no
# MAGIC se cambia, y `CATALOG` lo crea el profesor — solo confirma con tu docente
# MAGIC que el nombre del semestre sea el que está en `config.py`.
# MAGIC
# MAGIC ## Permisos que necesitas
# MAGIC `USE CATALOG` sobre el catálogo compartido y ser dueño de tu schema. **No**
# MAGIC necesitas (ni tienes) `CREATE SCHEMA`. Si algo falta, pídeselo al profesor.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validar la configuración

# COMMAND ----------

assert CATALOG and SCHEMA and VOLUME, (
    "CATALOG, SCHEMA y VOLUME deben tener valor. "
    "Revisa el bloque editable de config.py y pon tu nombre_apellido en SCHEMA."
)

print("✓ Configuración cargada")
print(f"  Catálogo (compartido): {CATALOG}")
print(f"  Schema (tuyo):         {SCHEMA}")
print(f"  Volume (tuyo):         {VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Activar el catálogo compartido
# MAGIC
# MAGIC Si el catálogo no existe o no tienes permiso, este paso **falla a propósito**:
# MAGIC crear el catálogo es tarea del profesor.

# COMMAND ----------

try:
    spark.sql(f"USE CATALOG `{CATALOG}`")
    print(f"✓ Catálogo activo: {CATALOG}")
except Exception as e:
    raise RuntimeError(
        f"No se pudo activar el catálogo '{CATALOG}'.\n"
        f"Este notebook NO crea catálogos: el catálogo del curso lo crea el profesor.\n"
        f"Verifica que el nombre en config.py sea el del semestre y que tengas el "
        f"permiso USE CATALOG. Si el problema persiste, contacta al docente.\n"
        f"Error original: {e}"
    ) from e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tu schema de trabajo
# MAGIC
# MAGIC Solo se verifica que exista: los estudiantes no tienen `CREATE SCHEMA`
# MAGIC sobre el catálogo, así que un `CREATE SCHEMA` fallaría con
# MAGIC `PERMISSION_DENIED`. Ojo: un schema sobre el que no tienes ningún permiso
# MAGIC es invisible para ti, así que "no existe" también puede significar "no es tuyo".

# COMMAND ----------

if not spark.catalog.databaseExists(f"`{CATALOG}`.`{SCHEMA}`"):
    raise RuntimeError(
        f"No encuentro el schema {CATALOG}.{SCHEMA}.\n"
        f"Revisa que SCHEMA en config.py sea tu nombre_apellido exacto (el que ves en\n"
        f"Catalog Explorer dentro de {CATALOG}). Si no aparece, pídele al profesor que\n"
        f"lo cree y te lo asigne como dueño: tú no puedes crear schemas en el catálogo."
    )
print(f"  ✓ Schema: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comprobar que puedes crear tablas administradas
# MAGIC
# MAGIC Todas las capas del pipeline son tablas administradas, y Unity Catalog
# MAGIC guarda sus datos en la *managed location* del catálogo (o, si no tiene,
# MAGIC en la del metastore). Si ninguna existe, `saveAsTable` falla recién en
# MAGIC bronze con un error de storage poco claro. Esta celda crea y borra una
# MAGIC tabla de prueba en tu schema para detectarlo aquí.

# COMMAND ----------

_probe_table = f"`{CATALOG}`.`{SCHEMA}`.`_setup_probe`"

try:
    spark.sql(f"CREATE OR REPLACE TABLE {_probe_table} (ok BOOLEAN)")
    spark.sql(f"INSERT INTO {_probe_table} VALUES (true)")
    assert spark.table(_probe_table).count() == 1
except Exception as e:
    raise RuntimeError(
        f"No se pudo crear una tabla administrada en {CATALOG}.{SCHEMA}.\n"
        f"Lo más probable es que el catálogo no tenga managed location y el\n"
        f"metastore tampoco. Eso lo configura el profesor (ADMIN_SETUP.md, Paso 5:\n"
        f"CREATE CATALOG ... MANAGED LOCATION). Compártele este error.\n"
        f"Error original: {e}"
    ) from e
finally:
    # Solo borra la tabla de prueba que creó esta misma celda
    spark.sql(f"DROP TABLE IF EXISTS {_probe_table}")

print(f"  ✓ Puedes crear tablas administradas en {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tu Volume `datos`
# MAGIC
# MAGIC Normalmente ya existe (lo usaste en módulos anteriores); si no, se crea.
# MAGIC Las capas medallion **no** van aquí: son tablas administradas de Unity
# MAGIC Catalog dentro de tu schema, y se crean solas la primera vez que cada
# MAGIC notebook hace `saveAsTable`. Este módulo solo usa el Volume para los
# MAGIC checkpoints de Structured Streaming, bajo `_checkpoints/ny_taxi`.

# COMMAND ----------

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`
      COMMENT 'Volume de trabajo del estudiante'
""")
print(f"  ✓ Volume: {DATALAKE_BASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Nombres resueltos

# COMMAND ----------

print("Landing (solo lectura, del profesor)")
print(f"  trips:            {LANDING_TRIPS_PATH}")
print(f"  zones:            {LANDING_ZONES_PATH}")
print("\nTus tablas (Unity Catalog)")
print(f"  bronze trips:     {T_BRONZE_TRIPS}")
print(f"  bronze zones:     {T_BRONZE_ZONES}")
print(f"  silver trips:     {T_SILVER_TRIPS}")
print(f"  silver rejected:  {T_SILVER_REJECTED}")
print(f"  silver enriched:  {T_SILVER_TRIPS_ENRICHED}")
print(f"  silver calidad:   {T_SILVER_QUALITY_METRICS}")
print(f"  gold revenue:     {T_GOLD_REVENUE_BY_ZONE}")
print(f"  gold diario:      {T_GOLD_DAILY_METRICS}")
print(f"  gold horario:     {T_GOLD_HOURLY_DEMAND}")
print(f"  gold ml_features: {T_GOLD_ML_FEATURES}")
print(f"  ml predicciones:  {T_ML_PREDICTIONS}")
print(f"  stream bronze:    {T_STREAM_BRONZE}")
print(f"  stream silver:    {T_STREAM_SILVER}")
print("\nVolume (solo archivos de estado)")
print(f"  checkpoints:      {CHECKPOINT_BASE}")
print(f"\nModelo en UC:       {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verificación

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN `{CATALOG}`.`{SCHEMA}`"))

# COMMAND ----------

# Confirmar que puedes leer el landing compartido
landing_files = dbutils.fs.ls(LANDING_TRIPS_PATH)
print(f"✓ Landing legible: {len(landing_files)} objetos en {LANDING_TRIPS_PATH}")
for f in landing_files[:5]:
    print(f"    {f.name}")
