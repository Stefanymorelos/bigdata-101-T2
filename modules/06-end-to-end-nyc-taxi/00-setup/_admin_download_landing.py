# Databricks notebook source
# MAGIC %md
# MAGIC # ADMIN ONLY — Descarga del dataset al landing (Volume UC)
# MAGIC
# MAGIC Descarga los Parquet mensuales de NYC TLC al Volume de landing ya existente.
# MAGIC Los estudiantes leen desde ahí.
# MAGIC
# MAGIC Los Volumes de UC se montan en `/Volumes/<catalog>/<schema>/<volume>/`
# MAGIC y son accesibles como filesystem local — sin restricciones de clusters
# MAGIC Shared/Serverless.
# MAGIC
# MAGIC Es idempotente: si un archivo ya existe, se salta.

# COMMAND ----------

import urllib.request
import os

# =============================================================================
# Configuración
# =============================================================================
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Paths de los Volumes de landing (ya creados)
CATALOG = 
TRIPS_DIR = f"/Volumes/{CATALOG}/default/nytaxi_landing"
ZONES_DIR = f"/Volumes/{CATALOG}/default/nytaxi_zones"

# Rango de años a descargar (inclusive). Cada año descarga los 12 meses.
# Los datos se publican con ~2 meses de retraso; meses futuros se saltan.
YEAR_START = 2023
YEAR_END = 2026

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Descargar parquets mensuales
# MAGIC
# MAGIC `urllib.request.urlretrieve` escribe directo a `/Volumes/...` que es
# MAGIC un path local válido en clusters UC. Sin `dbutils.fs.cp`, sin `/tmp`.
# MAGIC
# MAGIC Genera las URLs dinámicamente para el rango de años configurado.
# MAGIC Patrón: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`

# COMMAND ----------

from datetime import date

def generate_yellow_taxi_urls(year_start: int, year_end: int) -> list:
    """Genera la lista de (filename, url) para el rango de años dado."""
    urls = []
    today = date.today()
    for year in range(year_start, year_end + 1):
        for month in range(1, 13):
            # No generar URLs para meses futuros
            if date(year, month, 1) > today:
                break
            month_str = f"{year}-{month:02d}"
            filename = f"yellow_tripdata_{month_str}.parquet"
            url = f"{TLC_BASE_URL}/{filename}"
            urls.append((filename, url))
    return urls

file_list = generate_yellow_taxi_urls(YEAR_START, YEAR_END)
print(f"Archivos a verificar/descargar: {len(file_list)}")

# COMMAND ----------

os.makedirs(TRIPS_DIR, exist_ok=True)

downloaded = 0
skipped = 0
errors = []

for filename, url in file_list:
    dest = f"{TRIPS_DIR}/{filename}"

    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        size_mb = os.path.getsize(dest) / (1024 * 1024)
        print(f"  ⏭️  {filename:45s} ({size_mb:>7.1f} MB ya existe)")
        skipped += 1
        continue

    print(f"  ⬇️  {filename:45s} ...", end="", flush=True)
    try:
        urllib.request.urlretrieve(url, dest)
        size_mb = os.path.getsize(dest) / (1024 * 1024)
        print(f" {size_mb:>7.1f} MB ✅")
        downloaded += 1
    except Exception as e:
        print(f" ❌ {e}")
        errors.append((filename, str(e)))
        if os.path.exists(dest):
            os.remove(dest)

print(f"\n{'='*60}")
print(f"  Descargados: {downloaded}  |  Existentes: {skipped}  |  Errores: {len(errors)}")
print(f"{'='*60}")
if errors:
    print("\n⚠️  Archivos con error:")
    for fn, err in errors:
        print(f"    - {fn}: {err}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Descargar tabla de zonas

# COMMAND ----------

os.makedirs(ZONES_DIR, exist_ok=True)
zones_file = f"{ZONES_DIR}/taxi_zone_lookup.csv"

if os.path.exists(zones_file) and os.path.getsize(zones_file) > 0:
    print(f"  ✓ zones ya existe ({os.path.getsize(zones_file)} bytes)")
else:
    urllib.request.urlretrieve(ZONES_URL, zones_file)
    print(f"  ✓ zones descargado ({os.path.getsize(zones_file)} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación

# COMMAND ----------

print("Yellow trips:")
for f in os.listdir(TRIPS_DIR):
    size = os.path.getsize(f"{TRIPS_DIR}/{f}") / 1024 / 1024
    print(f"  {f:45s} {size:>8.1f} MB")

print(f"\nZones:")
for f in os.listdir(ZONES_DIR):
    size = os.path.getsize(f"{ZONES_DIR}/{f}") / 1024
    print(f"  {f:45s} {size:>8.1f} KB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prueba de lectura con Spark

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.read.parquet(TRIPS_DIR)
df.agg(
    F.count("*").alias("total_rows"),
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Los estudiantes lo leen así
# MAGIC
# MAGIC ```python
# MAGIC df = spark.read.parquet("/Volumes/maestria_bd_2026_01/default/nytaxi_landing")
# MAGIC zones = spark.read.option("header", True).csv("/Volumes/maestria_bd_2026_01/default/nytaxi_zones/taxi_zone_lookup.csv")
# MAGIC ```
