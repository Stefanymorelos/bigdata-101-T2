# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard NYC Taxi (versión notebook)
# MAGIC
# MAGIC Mismo tablero que `06-dashboard/*.sql`, pero sin SQL Warehouse: corre en un
# MAGIC cluster clásico y dibuja los gráficos con **Plotly**, que quedan escritos en
# MAGIC el código (no hay que configurarlos a mano en la UI). Útil cuando no quieres
# MAGIC encender un warehouse — los dashboards de AI/BI sí lo exigen.
# MAGIC
# MAGIC ## Cómo usarlo
# MAGIC 1. Corre **Run all**. La primera celda instala Plotly y reinicia Python.
# MAGIC 2. Arriba aparecen los widgets `fecha_desde`, `fecha_hasta` y `borough`.
# MAGIC    Cámbialos y vuelve a ejecutar (o Run all).
# MAGIC
# MAGIC Los filtros equivalen a los parámetros `:fecha` y `:borough` del dashboard.
# MAGIC Fecha vacía = sin límite por ese extremo; `borough` vacío = todos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Dependencias
# MAGIC
# MAGIC Plotly no viene en el runtime, así que se instala solo en este notebook (es
# MAGIC el único del módulo que dibuja gráficos). Los notebooks de datos no lo necesitan.

# COMMAND ----------

# MAGIC %pip install plotly==7.1.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../00-setup/config

# COMMAND ----------

from pyspark.sql import functions as F
import plotly.express as px

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtros
# MAGIC
# MAGIC `fecha_desde` / `fecha_hasta` en formato `YYYY-MM-DD`. Vacío = sin límite por ese extremo.
# MAGIC `borough` vacío = todos los boroughs.

# COMMAND ----------

dbutils.widgets.text("fecha_desde", "", "Fecha desde (YYYY-MM-DD)")
dbutils.widgets.text("fecha_hasta", "", "Fecha hasta (YYYY-MM-DD)")
dbutils.widgets.text("borough", "", "Borough (vacío = todos)")

_desde = dbutils.widgets.get("fecha_desde").strip()
_hasta = dbutils.widgets.get("fecha_hasta").strip()
_borough = dbutils.widgets.get("borough").strip()

def filtro_fecha(df, col="pickup_date"):
    if _desde:
        df = df.filter(F.col(col) >= F.lit(_desde).cast("date"))
    if _hasta:
        df = df.filter(F.col(col) <= F.lit(_hasta).cast("date"))
    return df

def filtro_borough(df, col="pickup_borough"):
    return df.filter(F.col(col) == _borough) if _borough else df

_subtitulo = f"desde={_desde or '—'}  hasta={_hasta or '—'}  borough={_borough or 'todos'}"
print(f"Filtros → {_subtitulo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. KPIs globales
# MAGIC
# MAGIC Tarjetas tipo *counter*. El % de propina usa solo pagos con tarjeta.

# COMMAND ----------

kpis = (
    filtro_fecha(spark.table(T_SILVER_TRIPS_ENRICHED)).agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("total_amount") / 1e6, 2).alias("total_revenue_millions"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
        F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
        F.round(F.avg(F.when(F.col("payment_type") == 1, F.col("tip_rate"))) * 100, 1).alias("avg_tip_pct"),
    ).first()
)

import plotly.graph_objects as go
_tarjetas = [
    ("Viajes totales", f"{kpis['total_trips']:,}"),
    ("Revenue (M USD)", f"${kpis['total_revenue_millions']:,.1f}"),
    ("Tarifa promedio", f"${kpis['avg_fare']:,.2f}"),
    ("Distancia prom. (mi)", f"{kpis['avg_distance_mi']:.2f}"),
    ("Duración prom. (min)", f"{kpis['avg_duration_min']:.1f}"),
    ("Propina % (tarjeta)", f"{kpis['avg_tip_pct']:.1f}%"),
]
fig = go.Figure()
for i, (titulo, valor) in enumerate(_tarjetas):
    fig.add_trace(go.Indicator(
        mode="number", value=0,
        number={"font": {"size": 1}},  # ocultamos el 0; el valor real va en el título
        title={"text": f"<b>{valor}</b><br><span style='font-size:0.8em;color:gray'>{titulo}</span>"},
        domain={"row": 0, "column": i},
    ))
fig.update_layout(grid={"rows": 1, "columns": len(_tarjetas)}, height=180,
                  title=f"KPIs globales — {_subtitulo}")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tendencia diaria
# MAGIC
# MAGIC Serie de viajes por día. Del 2023-09-21 al 2023-09-24 casi no hay viajes:
# MAGIC el hueco viene del parquet de la TLC, no del pipeline.

# COMMAND ----------

daily_pd = (
    filtro_fecha(spark.table(T_GOLD_DAILY_METRICS))
    .select("pickup_date", "total_trips", "total_revenue", "avg_fare")
    .orderBy("pickup_date")
    .toPandas()
)
fig = px.line(daily_pd, x="pickup_date", y="total_trips",
              title=f"Viajes por día — {_subtitulo}",
              labels={"pickup_date": "Fecha", "total_trips": "Viajes"})
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Top 20 zonas por revenue
# MAGIC
# MAGIC Tabla pre-agregada por zona: el filtro de fecha no aplica, el de `borough` sí.

# COMMAND ----------

zonas_pd = (
    filtro_borough(spark.table(T_GOLD_REVENUE_BY_ZONE))
    .select("pickup_zone", "pickup_borough", "total_trips", "total_revenue", "avg_fare", "avg_tip_pct")
    .orderBy(F.desc("total_revenue"))
    .limit(20)
    .toPandas()
)
fig = px.bar(zonas_pd.sort_values("total_revenue"), x="total_revenue", y="pickup_zone",
             orientation="h", color="pickup_borough",
             title=f"Top 20 zonas por revenue — borough={_borough or 'todos'}",
             labels={"total_revenue": "Revenue (USD)", "pickup_zone": "Zona", "pickup_borough": "Borough"})
fig.update_layout(height=600)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Heatmap: viajes promedio por hora × día de semana
# MAGIC
# MAGIC Promedio por día: suma de viajes dividida por los días distintos del rango
# MAGIC (el denominador no se filtra por borough).

# COMMAND ----------

rango_hd = filtro_fecha(spark.table(T_GOLD_HOURLY_DEMAND))
dias_por_dow = rango_hd.groupBy("pickup_dayofweek").agg(F.countDistinct("pickup_date").alias("n_dias"))

heat_pd = (
    filtro_borough(rango_hd)
    .groupBy("pickup_dayofweek", "pickup_hour")
    .agg(F.sum("trips").alias("trips"))
    .join(dias_por_dow, "pickup_dayofweek")
    .withColumn("avg_trips", F.round(F.col("trips") / F.col("n_dias"), 1))
    .toPandas()
)
_dias = {1: "Dom", 2: "Lun", 3: "Mar", 4: "Mié", 5: "Jue", 6: "Vie", 7: "Sáb"}
heat_pd["day_name"] = heat_pd["pickup_dayofweek"].map(_dias)
_orden = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
pivot = (heat_pd.pivot(index="day_name", columns="pickup_hour", values="avg_trips")
         .reindex(_orden))
fig = px.imshow(pivot, aspect="auto", color_continuous_scale="YlOrRd",
                title=f"Demanda promedio por hora y día — borough={_borough or 'todos'}",
                labels={"x": "Hora", "y": "Día", "color": "Viajes/día"})
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Top 20 pares origen→destino
# MAGIC
# MAGIC Excluye las zonas especiales 264 (Unknown) y 265 (Outside of NYC).

# COMMAND ----------

pares_pd = (
    filtro_fecha(spark.table(T_SILVER_TRIPS_ENRICHED))
    .filter(~F.col("PULocationID").isin(264, 265))
    .filter(~F.col("DOLocationID").isin(264, 265))
    .groupBy("pickup_zone", "dropoff_zone")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
    )
    .orderBy(F.desc("trips"))
    .limit(20)
    .toPandas()
)
pares_pd["par"] = pares_pd["pickup_zone"] + " → " + pares_pd["dropoff_zone"]
fig = px.bar(pares_pd.sort_values("trips"), x="trips", y="par", orientation="h",
             title=f"Top 20 pares origen→destino — {_subtitulo}",
             labels={"trips": "Viajes", "par": "Origen → Destino"})
fig.update_layout(height=600)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Distribución de propinas
# MAGIC
# MAGIC Solo pagos con tarjeta (`payment_type = 1`); en efectivo la propina no se registra.

# COMMAND ----------

_bucket = (
    F.when(F.col("tip_rate") == 0, "0%")
     .when(F.col("tip_rate") < 0.10, "< 10%")
     .when(F.col("tip_rate") < 0.15, "10-15%")
     .when(F.col("tip_rate") < 0.20, "15-20%")
     .when(F.col("tip_rate") < 0.25, "20-25%")
     .otherwise("25%+")
)
tips_pd = (
    filtro_fecha(spark.table(T_SILVER_TRIPS_ENRICHED))
    .filter(F.col("payment_type") == 1)
    .withColumn("tip_bucket", _bucket)
    .groupBy("tip_bucket")
    .agg(F.count("*").alias("trips"))
    .toPandas()
)
_orden_b = ["0%", "< 10%", "10-15%", "15-20%", "20-25%", "25%+"]
fig = px.bar(tips_pd, x="tip_bucket", y="trips", category_orders={"tip_bucket": _orden_b},
             title=f"Distribución del % de propina (tarjeta) — {_subtitulo}",
             labels={"tip_bucket": "% de propina", "trips": "Viajes"})
fig.show()
