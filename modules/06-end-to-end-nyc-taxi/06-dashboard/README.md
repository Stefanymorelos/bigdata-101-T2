# 06 - Dashboard

Queries SQL para construir un dashboard en **Databricks SQL**. El dashboard se crea desde la UI apuntando a un SQL Warehouse y cada widget usa una de las queries de esta carpeta.

## Dashboard sugerido: "NYC Taxi Operations"

| Widget | Tipo | Query |
|--------|------|-------|
| Trips totales | Counter | `01_kpi_totals.sql` |
| Revenue total | Counter | `01_kpi_totals.sql` |
| Trips diarios | Timeseries | `02_daily_trend.sql` |
| Top 10 zonas por revenue | Bar | `03_top_zones_revenue.sql` |
| Heatmap hora × día (viajes promedio por día) | Heatmap | `04_heatmap_demand.sql` |
| Top pares origen→destino | Table | `05_top_trip_pairs.sql` |
| Distribución de propinas | Histogram | `06_tip_distribution.sql` |

## Tablas que consumen las queries

Las capas son tablas administradas de Unity Catalog, así que las queries las leen por nombre directamente desde un SQL Warehouse, sin pasar por un notebook. Los nombres van **sin calificar**: en el editor SQL (o en el dataset del dashboard) selecciona el catálogo `20080_databricks` y tu schema, y se resuelven solos. Así la misma query sirve para cualquier estudiante.

| Tabla | La produce | Queries |
|-------|-----------|---------|
| `silver_yellow_trips_enriched` | `02-silver/02_silver_trips_enriched.py` | `01`, `05`, `06` |
| `gold_taxi_daily_metrics` | `03-gold/02_gold_daily_metrics.py` | `02` |
| `gold_taxi_revenue_by_zone` | `03-gold/01_gold_revenue_by_zone.py` | `03` |
| `gold_taxi_hourly_demand` | `03-gold/03_gold_hourly_demand.py` | `04` |


## Cómo armar el dashboard

1. Crear un dashboard de AI/BI en Databricks SQL con un SQL Warehouse como compute
2. Agregar cada `.sql` como dataset, con catálogo `20080_databricks` y tu schema seleccionados
3. Declarar los parámetros que piden:
   - `:fecha`, de tipo **Date Range** (queries `01`, `02`, `04`, `05`, `06`). Crea `:fecha.min` y `:fecha.max`, y cuando se elige "All" llega como `NULL`, así que no se filtra.
   - `:borough`, de tipo String (queries `03`, `04`). Si llega `NULL` ("All") se muestran todos los boroughs.
   - El % de propina (`avg_tip_pct`) se calcula solo con pagos con tarjeta (`payment_type = 1`), porque en efectivo y en Flex Fare la propina no se registra.
4. Crear cada widget con el tipo de gráfico de la tabla de arriba

Para compartir el dashboard con otra persona, dale `SELECT` sobre las tablas que usa (`GRANT SELECT ON TABLE ... TO ...`); con Delta por path eso no era posible a nivel de tabla.

## Databricks Marketplace y AI/BI

Una vez el dashboard funciona, se puede:
- Publicar como producto de datos vía **Delta Sharing**
- Agregar AI/BI Genie para que usuarios de negocio hagan preguntas en lenguaje natural
