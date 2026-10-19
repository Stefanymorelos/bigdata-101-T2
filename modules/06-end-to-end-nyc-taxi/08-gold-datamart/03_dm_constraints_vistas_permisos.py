# Databricks notebook source
# MAGIC %md
# MAGIC # Data mart Finanzas TLC — 03. Constraints, vistas y permisos
# MAGIC
# MAGIC **Parte 3** del ejercicio:
# MAGIC 1. `PRIMARY KEY` en cada dimensión y `FOREIGN KEY` de la tabla de hechos a cada dimensión.
# MAGIC 2. Vistas de consumo para Finanzas, con nombres legibles y sin códigos.
# MAGIC 3. Permisos: un usuario que hace el papel de Finanzas lee **solo** las vistas.
# MAGIC
# MAGIC Requisito: haber corrido `02_dm_fact_trips` **con la conciliación en verde**.

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ./_dm_common

# COMMAND ----------

def _name(table: str) -> str:
    """`cat`.`schema`.`tabla` → tabla"""
    return table.split(".")[-1].strip("`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Constraints
# MAGIC
# MAGIC El notebook es **idempotente**: se puede correr todos los días desde el workflow.
# MAGIC
# MAGIC 1. `DROP PRIMARY KEY IF EXISTS CASCADE` en cada dimensión. `CASCADE` borra también las FK de la tabla de
# MAGIC    hechos que la referencian.
# MAGIC 2. Se crean de nuevo las PK, ahora con **`RELY`** (ver la pregunta al final de la sección).
# MAGIC 3. Se crean las 7 FK. `dim_zone` recibe **dos** FK, una por rol: pickup y dropoff.
# MAGIC
# MAGIC Las columnas de las PK ya se declararon `NOT NULL` en el DDL de los notebooks 01 y 02.

# COMMAND ----------

USE_RELY = True  # Solo es seguro porque 02_dm_fact_trips verifica unicidad y huérfanas ANTES (ver DAG, Parte 6)

for dim_table, pk_col in DIM_PKS.items():
    spark.sql(f"ALTER TABLE {dim_table} DROP PRIMARY KEY IF EXISTS CASCADE")
    spark.sql(f"""
        ALTER TABLE {dim_table}
        ADD CONSTRAINT pk_{_name(dim_table)} PRIMARY KEY ({pk_col}) {'RELY' if USE_RELY else ''}
    """)
    print(f"✓ PK  {_name(dim_table)}({pk_col}){' RELY' if USE_RELY else ''}")

for fk_col, (dim_table, pk_col) in FACT_FKS.items():
    fk_name = f"fk_{_name(T_DM_FACT_TRIPS)}_{fk_col}"
    spark.sql(f"ALTER TABLE {T_DM_FACT_TRIPS} DROP CONSTRAINT IF EXISTS {fk_name}")
    spark.sql(f"""
        ALTER TABLE {T_DM_FACT_TRIPS}
        ADD CONSTRAINT {fk_name} FOREIGN KEY ({fk_col}) REFERENCES {dim_table} ({pk_col})
    """)
    print(f"✓ FK  fact_trips.{fk_col} → {_name(dim_table)}({pk_col})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación: las constraints quedaron registradas
# MAGIC
# MAGIC Se esperan **6 PRIMARY KEY** (una por dimensión) y **7 FOREIGN KEY** (en `fact_trips`). También se pueden ver
# MAGIC en Catalog Explorer, en la pestaña *Overview* de cada tabla, o como diagrama en
# MAGIC *fact_trips → View relationships*.

# COMMAND ----------

constraints = spark.sql(f"""
    SELECT tc.table_name, tc.constraint_type, tc.constraint_name,
           concat_ws(', ', collect_list(kcu.column_name)) AS columns
    FROM `{CATALOG}`.information_schema.table_constraints tc
    LEFT JOIN `{CATALOG}`.information_schema.key_column_usage kcu
      ON  tc.constraint_catalog = kcu.constraint_catalog
      AND tc.constraint_schema  = kcu.constraint_schema
      AND tc.constraint_name    = kcu.constraint_name
    WHERE tc.table_schema = '{SCHEMA}'
      AND tc.table_name LIKE 'gold_taxi_dm_%'
    GROUP BY tc.table_name, tc.constraint_type, tc.constraint_name
    ORDER BY tc.constraint_type DESC, tc.table_name, tc.constraint_name
""")
display(constraints)

counts = {r.constraint_type: r["count"] for r in constraints.groupBy("constraint_type").count().collect()}
assert counts.get("PRIMARY KEY", 0) == 6, f"Se esperaban 6 PK y hay {counts.get('PRIMARY KEY', 0)}"
assert counts.get("FOREIGN KEY", 0) == 7, f"Se esperaban 7 FK y hay {counts.get('FOREIGN KEY', 0)}"
print("✓ 6 PRIMARY KEY + 7 FOREIGN KEY registradas en information_schema")

# COMMAND ----------

# Detalle de las FK (a qué tabla apuntan)
display(spark.sql(f"""
    SELECT constraint_name, unique_constraint_name AS pk_referenciada
    FROM `{CATALOG}`.information_schema.referential_constraints
    WHERE constraint_schema = '{SCHEMA}' AND constraint_name LIKE 'fk_gold_taxi_dm_%'
    ORDER BY constraint_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta: si en Databricks las PK/FK son informativas (no se hacen cumplir al escribir), ¿para qué sirven? ¿Qué cambia con `RELY` y qué riesgo se asume?
# MAGIC
# MAGIC **Para qué sirven aunque no se hagan cumplir:**
# MAGIC
# MAGIC - **Documentan el modelo dentro del catálogo.** Cualquiera que abra Catalog Explorer ve cuál es la llave de cada
# MAGIC   dimensión y cómo se une la tabla de hechos, incluso como diagrama entidad-relación. El modelo estrella deja de vivir
# MAGIC   solo en la cabeza de quien lo diseñó.
# MAGIC - **Las herramientas las usan.** Power BI, Tableau, los dashboards de Databricks y Genie (AI/BI) leen las PK/FK
# MAGIC   para proponer los joins correctos automáticamente, y así un analista de Finanzas no une mal las tablas.
# MAGIC - **Expresan un contrato de calidad.** Lo que se declara es lo que el pipeline promete. La conciliación del notebook 02
# MAGIC   es la que **hace cumplir** ese contrato: unicidad y cero huérfanas.
# MAGIC - En cambio, `NOT NULL` y `CHECK` **sí se hacen cumplir** en Delta. Por eso las llaves son `NOT NULL`: un `NULL`
# MAGIC   en una llave hace fallar la escritura.
# MAGIC
# MAGIC **Qué cambia con `RELY`:** se le dice al optimizador que **puede confiar** en que la PK es verdadera y usarla para
# MAGIC reescribir consultas (requiere cómputo con Photon, como un SQL warehouse o serverless):
# MAGIC
# MAGIC - **Eliminación de joins.** En un `LEFT JOIN` de la tabla de hechos con una dimensión por su PK, si la consulta no usa
# MAGIC   ninguna columna de la dimensión, el join se elimina. Cada fila de hechos encuentra como máximo una fila de la
# MAGIC   dimensión, así que el join no cambia el resultado. Esto pasa a menudo con las vistas que unen las 7 dimensiones
# MAGIC   pero se consultan pidiendo solo 2 o 3 columnas.
# MAGIC - **Eliminación de agregaciones.** Un `DISTINCT` o `GROUP BY` sobre la PK se omite porque ya es única.
# MAGIC
# MAGIC **Riesgo:** como Databricks no verifica la constraint, **si los datos la violan, las consultas dan resultados
# MAGIC incorrectos sin ningún error**. Por ejemplo, si `dim_zone` tuviera la zona 132 duplicada, la misma consulta daría
# MAGIC un `COUNT(*)` distinto según si el optimizador eliminó el join o no. Por eso `RELY` solo es responsable cuando el
# MAGIC pipeline garantiza la unicidad **antes** de que alguien consulte. En este mart eso lo hace la conciliación del
# MAGIC notebook 02, que falla el workflow si hay duplicados o huérfanas y así impide que este notebook y las vistas se publiquen.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Vistas de consumo
# MAGIC
# MAGIC Son las **únicas** que verá Finanzas. Usan nombres legibles en español y no exponen códigos.
# MAGIC
# MAGIC | Vista | Grano | Uso |
# MAGIC |---|---|---|
# MAGIC | `gold_taxi_dm_v_revenue_mensual` | año × mes × borough de pickup × `service_zone` de pickup | Revenue, propinas y recargos (pregunta 1 de la Parte 4) |
# MAGIC | `gold_taxi_dm_v_propinas_tarjeta` | borough de pickup × franja del día × fin de semana, **solo tarjeta** | Propinas (pregunta 4) |
# MAGIC | `gold_taxi_dm_v_viajes` | un viaje, con todas las dimensiones resueltas a texto | Consultas *ad hoc* de Finanzas sin tocar tablas |
# MAGIC
# MAGIC Decisiones:
# MAGIC
# MAGIC - Los montos se suman como `DECIMAL(18,2)`. Así son exactos al centavo y se pueden volver a sumar sin errores de redondeo.
# MAGIC - Las vistas agregadas solo exponen **medidas aditivas** (sumas y conteos) y, cuando hay un ratio, también su
# MAGIC   numerador y su denominador. Quien agrupe la vista a un nivel más alto debe recalcular el ratio
# MAGIC   (`SUM(propinas) / SUM(viajes)`) y no promediar promedios.
# MAGIC - Se usa `LEFT JOIN` contra las dimensiones. No pierde filas (gracias al `-1`) y, con `RELY`, permite eliminar
# MAGIC   las dimensiones que una consulta no usa.
# MAGIC - `recargos_total` = congestión + aeropuerto + CBD, la misma definición de la pregunta 6. `COALESCE` evita que
# MAGIC   un `NULL` de Flex Fare anule la suma de la fila.

# COMMAND ----------

def money(col: str) -> str:
    return f"CAST({col} AS DECIMAL(18,2))"

STAR_FROM = f"""
    FROM {T_DM_FACT_TRIPS} f
    LEFT JOIN {T_DM_DIM_DATE}         d  ON f.pickup_date_key  = d.date_key
    LEFT JOIN {T_DM_DIM_TIME}         t  ON f.pickup_hour_key  = t.hour_key
    LEFT JOIN {T_DM_DIM_ZONE}         pu ON f.pickup_zone_key  = pu.zone_key
    LEFT JOIN {T_DM_DIM_ZONE}         dof ON f.dropoff_zone_key = dof.zone_key
    LEFT JOIN {T_DM_DIM_PAYMENT_TYPE} p  ON f.payment_type_key = p.payment_type_key
    LEFT JOIN {T_DM_DIM_RATE_CODE}    r  ON f.rate_code_key    = r.rate_code_key
    LEFT JOIN {T_DM_DIM_VENDOR}       v  ON f.vendor_key       = v.vendor_key
"""

spark.sql(f"""
CREATE OR REPLACE VIEW {T_DM_V_REVENUE_MENSUAL}
COMMENT 'Finanzas TLC: revenue, propinas y recargos por mes y borough/service_zone de pickup'
AS
SELECT
    d.year                                                   AS anio,
    d.month                                                  AS mes,
    d.month_name                                             AS nombre_mes,
    pu.borough                                               AS borough_pickup,
    pu.service_zone                                          AS service_zone_pickup,
    COUNT(*)                                                 AS viajes,
    SUM({money('f.total_amount')})                           AS revenue,
    SUM({money('f.fare_amount')})                            AS tarifa_base,
    SUM({money('f.tip_amount')})                             AS propinas,
    SUM({money('f.congestion_surcharge')})                   AS recargo_congestion,
    SUM({money('f.airport_fee')})                            AS recargo_aeropuerto,
    SUM({money('f.cbd_congestion_fee')})                     AS recargo_cbd,
    SUM({money('COALESCE(f.congestion_surcharge, 0) + COALESCE(f.airport_fee, 0) + f.cbd_congestion_fee')})
                                                             AS recargos_total
FROM {T_DM_FACT_TRIPS} f
LEFT JOIN {T_DM_DIM_DATE} d  ON f.pickup_date_key = d.date_key
LEFT JOIN {T_DM_DIM_ZONE} pu ON f.pickup_zone_key = pu.zone_key
GROUP BY d.year, d.month, d.month_name, pu.borough, pu.service_zone
""")
print(f"✓ {T_DM_V_REVENUE_MENSUAL}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {T_DM_V_PROPINAS_TARJETA}
COMMENT 'Finanzas TLC: propinas con tarjeta (único medio donde se registran) por borough, franja del día y fin de semana'
AS
SELECT
    pu.borough                                                        AS borough_pickup,
    t.day_part                                                        AS franja_dia,
    d.is_weekend                                                      AS es_fin_de_semana,
    COUNT(*)                                                          AS viajes_tarjeta,
    SUM({money('f.tip_amount')})                                      AS propinas_total,
    SUM({money('f.fare_amount')})                                     AS tarifa_total,
    ROUND(AVG(f.tip_amount), 2)                                       AS propina_promedio,
    ROUND(100 * SUM(f.tip_amount) / NULLIF(SUM(f.fare_amount), 0), 2) AS propina_pct_tarifa
FROM {T_DM_FACT_TRIPS} f
JOIN      {T_DM_DIM_PAYMENT_TYPE} p  ON f.payment_type_key = p.payment_type_key
LEFT JOIN {T_DM_DIM_DATE}         d  ON f.pickup_date_key  = d.date_key
LEFT JOIN {T_DM_DIM_TIME}         t  ON f.pickup_hour_key  = t.hour_key
LEFT JOIN {T_DM_DIM_ZONE}         pu ON f.pickup_zone_key  = pu.zone_key
WHERE p.tip_is_recorded
GROUP BY pu.borough, t.day_part, d.is_weekend
""")
print(f"✓ {T_DM_V_PROPINAS_TARJETA}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {T_DM_V_VIAJES}
COMMENT 'Finanzas TLC: un viaje por fila, con todas las dimensiones en texto legible (sin códigos)'
AS
SELECT
    -- Fecha y hora
    d.full_date           AS fecha,
    d.year                AS anio,
    d.quarter             AS trimestre,
    d.month               AS mes,
    d.month_name          AS nombre_mes,
    d.day_name            AS dia_semana,
    d.is_weekend          AS es_fin_de_semana,
    d.is_holiday          AS es_festivo,
    d.holiday_name        AS festivo,
    t.hour_key            AS hora,
    t.hour_label          AS franja_hora,
    t.day_part            AS franja_dia,
    t.is_rush_hour        AS es_hora_pico,
    -- Origen y destino (dim_zone en sus dos roles)
    pu.borough            AS borough_pickup,
    pu.zone               AS zona_pickup,
    pu.service_zone       AS service_zone_pickup,
    pu.is_airport         AS pickup_en_aeropuerto,
    pu.is_special_zone    AS pickup_zona_especial,
    dof.borough            AS borough_dropoff,
    dof.zone               AS zona_dropoff,
    dof.service_zone       AS service_zone_dropoff,
    dof.is_airport         AS dropoff_en_aeropuerto,
    dof.is_special_zone    AS dropoff_zona_especial,
    -- Pago, tarifa y proveedor
    p.payment_type_desc   AS forma_pago,
    p.payment_group       AS grupo_pago,
    p.tip_is_recorded     AS propina_registrada,
    r.rate_code_desc      AS tipo_tarifa,
    r.is_airport_rate     AS tarifa_aeropuerto,
    v.vendor_name         AS proveedor,
    -- Medidas
    f.passenger_count     AS pasajeros,
    f.trip_distance       AS distancia_millas,
    f.trip_duration_min   AS duracion_min,
    f.fare_amount         AS tarifa_base,
    f.extra               AS extra,
    f.mta_tax             AS impuesto_mta,
    f.tip_amount          AS propina,
    f.tolls_amount        AS peajes,
    f.improvement_surcharge AS recargo_mejoras,
    f.congestion_surcharge  AS recargo_congestion,
    f.airport_fee           AS recargo_aeropuerto,
    f.cbd_congestion_fee    AS recargo_cbd,
    f.total_amount          AS total
{STAR_FROM}
""")
print(f"✓ {T_DM_V_VIAJES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación de las vistas: cuadran con la tabla de hechos

# COMMAND ----------

fact_tot = spark.sql(f"SELECT COUNT(*) AS n, SUM(CAST(total_amount AS DECIMAL(38,2))) AS s FROM {T_DM_FACT_TRIPS}").first()
rev_tot = spark.sql(f"SELECT SUM(viajes) AS n, SUM(revenue) AS s FROM {T_DM_V_REVENUE_MENSUAL}").first()
via_tot = spark.sql(f"SELECT COUNT(*) AS n FROM {T_DM_V_VIAJES}").first()
card_fact = spark.sql(f"SELECT COUNT(*) AS n FROM {T_DM_FACT_TRIPS} WHERE payment_type_key = 1").first().n
card_view = spark.sql(f"SELECT SUM(viajes_tarjeta) AS n FROM {T_DM_V_PROPINAS_TARJETA}").first().n

assert rev_tot.n == fact_tot.n and rev_tot.s == fact_tot.s, "v_revenue_mensual no cuadra con fact_trips"
assert via_tot.n == fact_tot.n, "v_viajes no tiene las mismas filas que fact_trips"
assert card_view == card_fact, "v_propinas_tarjeta no cuadra con los viajes con tarjeta"
print(f"✓ Vistas cuadran: {fact_tot.n:,} viajes, revenue {fact_tot.s:,}")

display(spark.sql(f"SELECT * FROM {T_DM_V_REVENUE_MENSUAL} ORDER BY anio DESC, mes DESC, revenue DESC LIMIT 20"))
display(spark.sql(f"SELECT * FROM {T_DM_V_PROPINAS_TARJETA} ORDER BY borough_pickup, franja_dia, es_fin_de_semana"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Permisos (en parejas)
# MAGIC
# MAGIC La prueba se hizo con **Valentina Rendón** (`valentina.rendonc@upb.edu.co`), que hace el papel de un usuario de
# MAGIC Finanzas. Se le otorga:
# MAGIC
# MAGIC - `USE SCHEMA` sobre el schema `stefany_morelos`. Ya tenía `USE CATALOG` sobre el catálogo del curso.
# MAGIC - `SELECT` **solo sobre las 3 vistas**. **Nada** sobre `fact_trips`, las dimensiones ni silver.
# MAGIC
# MAGIC En una ejecución anterior también se le otorgaron estos permisos a Santiago Mendoza. Por eso su `USE SCHEMA` sigue
# MAGIC apareciendo en `SHOW GRANTS ON SCHEMA`, pero ya no tiene `SELECT` sobre las vistas: `CREATE OR REPLACE VIEW` (sección 3.2)
# MAGIC **reinicia los permisos de la vista** cada vez que se recrea. Por esa razón los `GRANT` van en este mismo notebook,
# MAGIC justo después de crear las vistas, para que se vuelvan a aplicar en cada ejecución del workflow.
# MAGIC
# MAGIC El usuario se recibe por el widget `companero`. Si el widget queda vacío, no se otorga nada, así que el notebook
# MAGIC también puede correr dentro del workflow sin tocar permisos.

# COMMAND ----------

dbutils.widgets.text("companero", "valentina.rendonc@upb.edu.co", "Usuario de prueba (vacío = no otorgar)")
PARTNER = dbutils.widgets.get("companero").strip()

MART_VIEWS = [T_DM_V_REVENUE_MENSUAL, T_DM_V_PROPINAS_TARJETA, T_DM_V_VIAJES]

if PARTNER:
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA `{CATALOG}`.`{SCHEMA}` TO `{PARTNER}`")
    for v in MART_VIEWS:
        spark.sql(f"GRANT SELECT ON TABLE {v} TO `{PARTNER}`")
    print(f"✓ {PARTNER}: USE SCHEMA + SELECT en {len(MART_VIEWS)} vistas")
else:
    print("Widget 'companero' vacío: no se otorgan permisos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidencia: `SHOW GRANTS`
# MAGIC
# MAGIC El usuario de prueba aparece con `USE SCHEMA` en el schema y con `SELECT` en las vistas, y **no** aparece
# MAGIC en `fact_trips` ni en silver.

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA `{CATALOG}`.`{SCHEMA}`"))

# COMMAND ----------

for v in MART_VIEWS:
    print(f"── SHOW GRANTS ON {_name(v)}")
    display(spark.sql(f"SHOW GRANTS ON TABLE {v}"))

# COMMAND ----------

for t in [T_DM_FACT_TRIPS, T_SILVER_TRIPS_ENRICHED]:
    print(f"── SHOW GRANTS ON {_name(t)}  (el usuario de prueba NO debe aparecer)")
    display(spark.sql(f"SHOW GRANTS ON TABLE {t}"))

if PARTNER:
    for t in [T_DM_FACT_TRIPS, T_SILVER_TRIPS_ENRICHED]:
        g = spark.sql(f"SHOW GRANTS `{PARTNER}` ON TABLE {t}")
        # SHOW GRANTS también lista lo heredado del schema (el USE SCHEMA otorgado arriba, ObjectType = SCHEMA).
        # Lo que no debe existir es un permiso otorgado directamente sobre la tabla.
        direct = g.filter(F.col("ObjectType") == "TABLE").count()
        assert direct == 0, f"{PARTNER} tiene permisos directos sobre {_name(t)}"
    print(f"✓ {PARTNER} no tiene permisos directos sobre fact_trips ni silver (solo hereda USE SCHEMA)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prueba desde la cuenta del usuario de Finanzas
# MAGIC
# MAGIC Valentina ejecutó estas consultas desde su cuenta, en el **SQL Editor** con el *Serverless Starter Warehouse*
# MAGIC (también están en `prueba_companero.sql`):
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM `20080_databricks`.`stefany_morelos`.gold_taxi_dm_v_revenue_mensual LIMIT 10;
# MAGIC SELECT * FROM `20080_databricks`.`stefany_morelos`.gold_taxi_dm_fact_trips LIMIT 10;
# MAGIC SELECT * FROM `20080_databricks`.`stefany_morelos`.silver_yellow_trips_enriched LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC **Evidencia y hallazgo**
# MAGIC
# MAGIC | Consulta (desde la cuenta de Valentina) | Resultado esperado | Resultado obtenido |
# MAGIC |---|---|---|
# MAGIC | `gold_taxi_dm_v_revenue_mensual` | Lee la vista | ✓ Lee la vista (10 filas: año, mes, borough, `service_zone`…) |
# MAGIC | `gold_taxi_dm_fact_trips` | `PERMISSION_DENIED` | ✗ **Lee la tabla** (10 filas con las llaves `pickup_date_key`, `pickup_zone_key`…) |
# MAGIC | `silver_yellow_trips_enriched` | `PERMISSION_DENIED` | ✗ **Lee la tabla** (10 filas con `PULocationID`, `tpep_pickup_datetime`…) |
# MAGIC
# MAGIC Capturas de la prueba (carpeta `evidencia/`):
# MAGIC
# MAGIC **1. Vista `gold_taxi_dm_v_revenue_mensual`: se lee ✓**
# MAGIC
# MAGIC ![Valentina lee la vista](./evidencia/1_valentina_vista_revenue_mensual.png)
# MAGIC
# MAGIC **2. Tabla `gold_taxi_dm_fact_trips`: también se lee (se esperaba `PERMISSION_DENIED`)**
# MAGIC
# MAGIC ![Valentina lee fact_trips](./evidencia/2_valentina_fact_trips.png)
# MAGIC
# MAGIC **3. Tabla `silver_yellow_trips_enriched`: también se lee (se esperaba `PERMISSION_DENIED`)**
# MAGIC
# MAGIC ![Valentina lee silver](./evidencia/3_valentina_silver.png)
# MAGIC
# MAGIC **4. `SHOW GRANTS` sobre el schema y las vistas**
# MAGIC
# MAGIC ![SHOW GRANTS](./evidencia/4_show_grants.png)
# MAGIC
# MAGIC La vista funciona como se diseñó, pero las tablas internas también quedan visibles. La causa está en los
# MAGIC `SHOW GRANTS` de arriba: además de los permisos otorgados en este notebook, el grupo **`NRC-20080`** tiene
# MAGIC **`ALL PRIVILEGES`** sobre todo el schema `stefany_morelos`. El profesor lo configuró así para que los estudiantes del
# MAGIC curso puedan usar el clúster compartido, y los compañeros pertenecen a ese grupo. El `SHOW GRANTS` filtrado por
# MAGIC usuario confirma que el usuario de prueba no tiene permisos **directos** sobre `fact_trips` ni silver: el acceso
# MAGIC viene del grupo.
# MAGIC
# MAGIC La prueba recíproca dio el mismo resultado: desde la cuenta de Stefany, sobre el mart de Valentina
# MAGIC (`valentina_rendon`), las vistas se leen y `fact_trips` también.
# MAGIC
# MAGIC **Por qué pasa:** en Unity Catalog los permisos **se suman** y no existe un "denegar". Un usuario tiene la unión de lo
# MAGIC que se le otorga a él y a todos sus grupos. Como `NRC-20080` tiene `ALL PRIVILEGES` sobre el schema, el permiso
# MAGIC heredado alcanza a todas las tablas, y el `SELECT` limitado a las vistas no lo restringe. Por eso la prueba no puede
# MAGIC mostrar el `PERMISSION_DENIED` en este workspace.
# MAGIC
# MAGIC **Cómo se garantizaría en un entorno real** que Finanzas solo vea las vistas:
# MAGIC 1. Los usuarios de Finanzas no deben pertenecer a ningún grupo con privilegios sobre el schema completo. Se les otorga
# MAGIC    acceso a través de un grupo propio (por ejemplo `finanzas_tlc`) que solo tenga `USE SCHEMA` y `SELECT` sobre las vistas.
# MAGIC 2. Mejor aún, las vistas de consumo se publican en un **schema separado** (por ejemplo `gold_taxi_consumo`) y Finanzas
# MAGIC    solo recibe permisos sobre ese schema. Así, un permiso amplio sobre el schema de trabajo nunca les da acceso a las
# MAGIC    tablas internas.
# MAGIC 3. Los permisos amplios a nivel de schema o catálogo se reservan para el equipo de ingeniería.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta: ¿por qué el compañero lee la vista aunque no tenga `SELECT` sobre las tablas que la vista consulta?
# MAGIC
# MAGIC Porque en Unity Catalog una vista se ejecuta con los **permisos de su dueño** (*definer's rights*), no con los de
# MAGIC quien la consulta.
# MAGIC
# MAGIC - Cuando el compañero hace `SELECT` sobre la vista, Unity Catalog verifica **sus** permisos solo sobre la vista
# MAGIC   (`SELECT`) y sobre los contenedores (`USE CATALOG`, `USE SCHEMA`).
# MAGIC - Para leer las tablas que la vista usa por dentro (`fact_trips`, `dim_*`), Unity Catalog verifica los permisos
# MAGIC   del **dueño de la vista**, que soy yo y sí tengo `SELECT` sobre ellas. A esto se le llama encadenamiento de
# MAGIC   propiedad (*ownership chaining*).
# MAGIC
# MAGIC Gracias a eso, la vista funciona como **capa de seguridad y como contrato**. El dueño decide exactamente qué columnas,
# MAGIC filas y nivel de agregación ve Finanzas: nombres legibles, sin códigos, sin silver. Puede cambiar las tablas por debajo
# MAGIC sin romper a los consumidores, y nunca entrega acceso a los datos crudos.
# MAGIC
# MAGIC Esto tiene dos consecuencias:
# MAGIC 1. Si el dueño pierde el `SELECT` sobre la tabla base, la vista deja de funcionar **para todos**.
# MAGIC 2. Quien tenga `SELECT` sobre `v_viajes` ve el detalle viaje a viaje. Por eso el diseño de la vista es la decisión
# MAGIC    de seguridad, y hay que exponer solo lo que el área necesita.
# MAGIC 3. La vista solo protege si es el **único** camino a los datos. Como se vio en la evidencia, si el usuario tiene un
# MAGIC    permiso más amplio por otro lado (un grupo con `ALL PRIVILEGES` sobre el schema), puede saltarse la vista y leer
# MAGIC    las tablas directamente.
