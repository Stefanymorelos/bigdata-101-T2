# 03 - Gold

Agregaciones de negocio y feature tables. Los datos de Silver se consolidan en métricas pre-calculadas listas para consumo.

## Conceptos que se enseñan aquí

- Agregaciones con `GROUP BY` sobre tablas grandes
- Feature engineering para ML (separado de los KPIs de negocio)
- Particionamiento y `ZORDER` según patrones de consulta
- Modelado dimensional básico (hechos + dimensiones)

## Orden de ejecución

Los cuatro son independientes entre sí; todos dependen solo de silver.

1. `01_gold_revenue_by_zone.py` — KPIs por zona de pickup. Lee `T_SILVER_TRIPS_ENRICHED`.
2. `02_gold_daily_metrics.py` — Serie diaria de viajes y revenue, más detección de días anómalos por z-score. Lee `T_SILVER_TRIPS_ENRICHED`.
3. `03_gold_hourly_demand.py` — Demanda por zona × fecha × hora (con día de semana). Guarda la fecha para poder filtrar por rango y promediar por día. Lee `T_SILVER_TRIPS_ENRICHED`.
4. `04_gold_ml_features.py` — Feature table para el modelo de predicción de duración. Lee `T_SILVER_TRIPS`.

Las zonas especiales 264 (`Unknown`) y 265 (`Outside of NYC`) se excluyen en los KPIs por zona (`01`, `03`) y en las features (`04`), pero **no** en las métricas diarias (`02`), que son totales del negocio y cuadran con silver.

## Salidas generadas

Tablas administradas de Unity Catalog en `<CATALOG>.<SCHEMA>`. La capa se distingue por el prefijo del nombre, no por un schema `gold` aparte.

| Constante de `config.py` | Tabla | Estado |
|--------------------------|-------|--------|
| `T_GOLD_ML_FEATURES` | `gold_taxi_ml_features` | La produce `04_gold_ml_features.py` |
| `T_GOLD_REVENUE_BY_ZONE` | `gold_taxi_revenue_by_zone` | La produce `01_gold_revenue_by_zone.py` |
| `T_GOLD_DAILY_METRICS` | `gold_taxi_daily_metrics` | La produce `02_gold_daily_metrics.py` |
| `T_GOLD_HOURLY_DEMAND` | `gold_taxi_hourly_demand` | La produce `03_gold_hourly_demand.py` |
