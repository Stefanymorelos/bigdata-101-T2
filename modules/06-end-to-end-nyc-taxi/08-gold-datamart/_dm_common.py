# Databricks notebook source
# MAGIC %md
# MAGIC # Utilidades compartidas del data mart
# MAGIC
# MAGIC Se importa con `%run ./_dm_common` **después** de `%run ../00-setup/config`.
# MAGIC No crea tablas: solo define constantes y funciones que usan los notebooks
# MAGIC `01_…` a `05_…` de esta carpeta.

# COMMAND ----------

from pyspark.sql import functions as F

# Llave del miembro desconocido en todas las dimensiones que lo necesitan
UNKNOWN_KEY = -1
UNKNOWN_DESC = "Desconocido / No aplica"

# Zonas especiales del lookup de la TLC: 264 = Unknown, 265 = Outside of NYC
SPECIAL_ZONE_IDS = [264, 265]

# Nombres exactos del lookup (verificados en 01_dm_dimensiones, sección 1.3)
AIRPORT_ZONE_NAMES = ["JFK Airport", "LaGuardia Airport", "Newark Airport"]

# Llaves foráneas de la tabla de hechos → (dimensión, llave en la dimensión)
FACT_FKS = {
    "pickup_date_key":  (T_DM_DIM_DATE,         "date_key"),
    "pickup_hour_key":  (T_DM_DIM_TIME,         "hour_key"),
    "pickup_zone_key":  (T_DM_DIM_ZONE,         "zone_key"),
    "dropoff_zone_key": (T_DM_DIM_ZONE,         "zone_key"),
    "payment_type_key": (T_DM_DIM_PAYMENT_TYPE, "payment_type_key"),
    "rate_code_key":    (T_DM_DIM_RATE_CODE,    "rate_code_key"),
    "vendor_key":       (T_DM_DIM_VENDOR,       "vendor_key"),
}

# Dimensión → su llave primaria
DIM_PKS = {
    T_DM_DIM_DATE:         "date_key",
    T_DM_DIM_TIME:         "hour_key",
    T_DM_DIM_ZONE:         "zone_key",
    T_DM_DIM_PAYMENT_TYPE: "payment_type_key",
    T_DM_DIM_RATE_CODE:    "rate_code_key",
    T_DM_DIM_VENDOR:       "vendor_key",
}

# COMMAND ----------

def write_mart_table(df, table: str, ddl: str, comment: str = "", cluster_by: str = None) -> None:
    """Crea la tabla (si no existe) con un DDL explícito y reemplaza su contenido.

    Por qué no `saveAsTable(mode="overwrite")` ni `CREATE OR REPLACE`:
    - El DDL explícito permite declarar `NOT NULL` en las llaves (requisito de una
      PRIMARY KEY) y los tipos exactos (INT en vez del LONG/DOUBLE que trae silver).
    - `INSERT OVERWRITE` cambia los datos pero conserva la tabla, así que las
      PRIMARY KEY / FOREIGN KEY de la Parte 3 sobreviven a cada re-ejecución.
      Reemplazar la tabla (CREATE OR REPLACE) chocaría con las FK que la referencian.
    - Si cambias el DDL, corre 01_dm_dimensiones con el widget `reset_mart = true`.
    """
    cluster = f" CLUSTER BY ({cluster_by})" if cluster_by else ""
    comment_sql = f" COMMENT '{comment}'" if comment else ""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({ddl}) USING DELTA{cluster}{comment_sql}")

    view = "_stage_" + table.split(".")[-1].strip("`")
    df.createOrReplaceTempView(view)
    cols = ", ".join(df.columns)
    spark.sql(f"INSERT OVERWRITE {table} ({cols}) SELECT {cols} FROM {view}")
    print(f"✓ {table}: {spark.table(table).count():,} filas")


def assert_unique_key(table: str, key: str) -> None:
    """Falla si la llave tiene nulos o duplicados."""
    row = spark.sql(f"""
        SELECT COUNT(*) AS n, COUNT(DISTINCT {key}) AS n_distinct,
               COUNT_IF({key} IS NULL) AS n_null
        FROM {table}
    """).first()
    assert row.n_null == 0, f"{table}.{key} tiene {row.n_null} nulos"
    assert row.n == row.n_distinct, f"{table}.{key} tiene {row.n - row.n_distinct} duplicados"
    print(f"✓ {table}.{key}: {row.n:,} filas, llave única y sin nulos")
