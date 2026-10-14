# 08 - Data mart dimensional en Gold (Finanzas TLC)

Modelo estrella para el área de Finanzas, construido desde silver. Esta carpeta va dentro de
`modules/06-end-to-end-nyc-taxi/`, al lado de `03-gold/`, para que el `%run ../00-setup/config` funcione.

## Antes de correr

1. `00-setup/config.py` ya tiene `CATALOG = "20080_databricks"`, `SCHEMA = "stefany_morelos"` y las constantes
   `T_DM_*` del mart.
2. Antes se ejecutan `00-setup/01_create_catalog`, `01-bronze/` (oct-2024 a mar-2025) y `02-silver/`.

## Orden de ejecución

| # | Notebook | Parte del ejercicio | Va en el workflow |
|---|---|---|---|
| — | `_dm_common.py` | Utilidades. No se corre solo: lo importan los demás con `%run` | — |
| 1 | `01_dm_dimensiones.py` | Parte 0 (diseño) + Parte 1 (6 dimensiones) | ✅ `dm_dimensions` |
| 2 | `02_dm_fact_trips.py` | Parte 2 (hechos, nulos, **conciliación**) + 5.3 (liquid clustering) | ✅ `dm_fact_trips` |
| 3 | `03_dm_constraints_vistas_permisos.py` | Parte 3 (PK/FK, vistas, permisos) | ✅ `dm_constraints_views` |
| 4 | `04_dm_preguntas_negocio.py` | Parte 4 (6 preguntas, solo con el mart) | ❌ análisis |
| 5 | `05_dm_rendimiento_orquestacion.py` | Parte 5 (silver vs. estrella) + Parte 6 (DAG) | ❌ análisis |
| — | `prueba_companero.sql` | Consultas de la prueba de permisos (Parte 3.3), desde la cuenta del compañero | — |
| — | `ENUNCIADO_exercise_datamart.md` | Enunciado del ejercicio | — |

## Si cambia el esquema de alguna tabla

Se ejecuta `01_dm_dimensiones` una vez con el widget `reset_mart = true` (borra y recrea el mart) y luego `02` y `03`.

## Diagrama

```mermaid
erDiagram
    fact_trips }o--|| dim_date         : pickup_date_key
    fact_trips }o--|| dim_time         : pickup_hour_key
    fact_trips }o--|| dim_zone         : "pickup_zone_key (rol pickup)"
    fact_trips }o--|| dim_zone         : "dropoff_zone_key (rol dropoff)"
    fact_trips }o--|| dim_payment_type : payment_type_key
    fact_trips }o--|| dim_rate_code    : rate_code_key
    fact_trips }o--|| dim_vendor       : vendor_key
```
