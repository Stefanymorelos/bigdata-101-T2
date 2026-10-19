# Ejercicio: Data mart dimensional en la capa Gold

## Objetivo

Diseñar y construir un **data mart en modelo estrella** dentro de la capa Gold, a partir de Silver, para que un área de negocio responda sus propias preguntas sin pedirle a ingeniería una tabla nueva cada vez.

Solo necesitas tener corridas las capas bronze y silver de `modules/06-end-to-end-nyc-taxi/` (`01-bronze/` y `02-silver/`).

## Contexto

Una forma común de construir Gold es crear una **tabla agregada por cada pregunta**: revenue por zona para un ranking, métricas diarias para una serie temporal, demanda por zona y hora para ver patrones de uso. Funciona, pero cada pregunta nueva exige otra tabla, otro notebook y otra tarea en el workflow, y las reglas de negocio (qué zonas excluir, qué viajes cuentan para propinas) se repiten en cada una.

Un **data mart** es un subconjunto del lakehouse modelado para un área de negocio. En la arquitectura medallion vive en Gold y se alimenta de Silver. Su forma más común es el **modelo estrella**: una tabla de hechos con las medidas del proceso de negocio, rodeada de dimensiones con los atributos para filtrar y agrupar.

### El cliente

El área de **Finanzas de la TLC** necesita analizar ingresos, propinas y recargos por fecha, hora, zona de origen, zona de destino, forma de pago, tipo de tarifa y proveedor. Quiere hacer sus consultas en SQL con nombres legibles (no códigos) y **no debe tener acceso a silver**.

### Fuentes

| Constante | Tabla | Uso |
|-----------|-------|-----|
| `T_SILVER_TRIPS_ENRICHED` | `silver_yellow_trips_enriched` | Fuente de la tabla de hechos |
| `T_BRONZE_ZONES` | `bronze_taxi_zones` | Fuente de la dimensión de zonas (`LocationID`, `Borough`, `Zone`, `service_zone`) |
| — | [Data dictionary de la TLC](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) | Descripciones de `payment_type`, `RatecodeID` y `VendorID` |

### Lo que trae silver

`T_SILVER_TRIPS_ENRICHED` tiene los viajes que pasaron las reglas de calidad, con:

- Llaves y códigos: `PULocationID`, `DOLocationID`, `payment_type`, `RatecodeID`, `VendorID`
- Tiempos: `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `pickup_date`, `pickup_hour`, `pickup_dayofweek` (1 = domingo … 7 = sábado), `is_weekend`, `is_rush_hour` (7–9 h y 17–19 h)
- Montos: `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee`, `cbd_congestion_fee`, `total_amount`
- Viaje: `passenger_count`, `trip_distance`, `trip_duration_min`
- Derivadas: `tip_rate`, `cost_per_mile`, `avg_speed_mph`
- Atributos de zona ya unidos (`pickup_borough`, `pickup_zone`, `dropoff_*`, ...). En el mart **no** los uses desde aquí: salen de `dim_zone`.

Particularidades que vas a tener que resolver en el diseño:

- **Zonas especiales 264 (`Unknown`) y 265 (`Outside of NYC`)**: silver las conserva porque son viajes reales, y vienen con texto, no con `NULL`.
- **Propinas**: solo se registran en pagos con tarjeta (`payment_type = 1`). En efectivo y en Flex Fare la propina queda en 0 aunque el pasajero la haya dado.
- **Flex Fare (`payment_type = 0`)**: la TLC publica estos viajes sin `passenger_count`, `RatecodeID`, `congestion_surcharge` ni `airport_fee`.
- **`cbd_congestion_fee`**: solo existe desde enero de 2025; antes viene en `NULL`.

Tus notebooks deben comenzar con:

```python
%run ../00-setup/config

from pyspark.sql import functions as F
```

### Nombres

Todo el mart vive en tu schema, con el prefijo `gold_taxi_dm_` (siguiendo la convención del módulo: la capa va en el prefijo y `taxi_` evita choques con el módulo 04). Agrega estas constantes a `00-setup/config.py`, derivadas de `TABLE_PREFIX` como las demás:

| Constante | Tabla |
|-----------|-------|
| `T_DM_DIM_DATE` | `gold_taxi_dm_dim_date` |
| `T_DM_DIM_TIME` | `gold_taxi_dm_dim_time` |
| `T_DM_DIM_ZONE` | `gold_taxi_dm_dim_zone` |
| `T_DM_DIM_PAYMENT_TYPE` | `gold_taxi_dm_dim_payment_type` |
| `T_DM_DIM_RATE_CODE` | `gold_taxi_dm_dim_rate_code` |
| `T_DM_DIM_VENDOR` | `gold_taxi_dm_dim_vendor` |
| `T_DM_FACT_TRIPS` | `gold_taxi_dm_fact_trips` |
| `T_DM_V_*` | `gold_taxi_dm_v_<nombre>` (vistas de consumo) |

---

## Parte 0: Diseño (antes de escribir código)

En una celda `%md` al inicio, documenta:

1. **El grano** de la tabla de hechos: qué representa cada fila, en una frase que complete "cada fila de esta tabla es ___". En modelado dimensional se le llama *grano* (en inglés, *grain*) y se decide antes que todo lo demás, porque fija qué columnas puede tener la tabla y qué preguntas permite responder.
   - Para este mart, cada fila es **un viaje**.
   - Explica por qué no conviene un grano agregado, por ejemplo "cada fila es una zona en un día". ¿Qué preguntas de la Parte 4 ya no podrías responder?
2. **El diagrama** del modelo estrella: texto, Mermaid o imagen.
3. **Cada dimensión**: llave, fuente y atributos.
4. **Cada medida**, clasificada como aditiva, semi-aditiva o no aditiva.

**Pregunta:** `tip_rate` y `avg_speed_mph` están en silver. ¿Deben ir como medidas en la tabla de hechos? ¿Qué pasa si alguien hace `SUM` o `AVG` de un ratio ya calculado a nivel de viaje en vez de recalcularlo desde sus componentes?

---

## Parte 1: Dimensiones

### 1.1 `dim_date`

- **No** la derives de `DISTINCT pickup_date`: genérala con una fila por día entre la fecha mínima y la máxima de silver (pista: `sequence` + `explode`).
- Columnas: `date_key` (INT con formato `yyyyMMdd`), `full_date`, `year`, `quarter`, `month`, `month_name`, `day_of_month`, `day_of_week` (1 = domingo … 7 = sábado, la misma convención que `pickup_dayofweek` en silver), `day_name`, `is_weekend`, `is_holiday`, `holiday_name`.
- Festivos: los federales de EE. UU. que caigan en el rango cargado. Arma tú la lista y documenta la fuente.

**Pregunta:** ¿qué se pierde si `dim_date` sale de los viajes y un día no tuvo ninguno?

### 1.2 `dim_time`

- 24 filas, una por hora: `hour_key` (0–23), `hour_label` (por ejemplo `07:00–07:59`), `day_part` (madrugada 0–5, mañana 6–11, tarde 12–17, noche 18–23) e `is_rush_hour`.
- `is_rush_hour` debe usar **la misma definición** que silver (7–9 h y 17–19 h). Verifícalo comparando contra silver.

### 1.3 `dim_zone`

- Fuente: `T_BRONZE_ZONES`. Columnas: `zone_key` (= `LocationID`), `borough`, `zone`, `service_zone`, `is_airport` e `is_special_zone`.
- `is_airport`: JFK, LaGuardia y Newark (verifica los nombres exactos en el lookup).
- `is_special_zone`: `true` para 264 (`Unknown`) y 265 (`Outside of NYC`). **No las borres**: la tabla de hechos debe conservar todos los viajes de silver.
- Es una **role-playing dimension**: la tabla de hechos la referencia dos veces (pickup y dropoff). Crea **una sola** tabla física.

### 1.4 `dim_payment_type`, `dim_rate_code`, `dim_vendor`

- Catálogos pequeños construidos a partir del data dictionary de la TLC: código, descripción y lo que consideres útil.
- Cubre los códigos que silver acepta: `payment_type` 0–6, `RatecodeID` 1–6, `VendorID` 1 y 2.
- `dim_payment_type` debe tener un atributo `tip_is_recorded` (`true` solo para tarjeta). Sin el mart, cada consulta de propinas tiene que repetir el filtro `payment_type = 1`; en el mart la regla vive en un solo lugar.
- Cada una incluye un **miembro desconocido** con llave `-1` y descripción `Desconocido / No aplica`.

**Pregunta:** ¿por qué un miembro `-1` es mejor que dejar la llave foránea en `NULL`?

---

## Parte 2: Tabla de hechos `fact_trips`

### Requisitos

- **Grano (qué representa cada fila):** un viaje. La tabla debe tener exactamente las mismas filas que `T_SILVER_TRIPS_ENRICHED`.
- **Llaves foráneas:** `pickup_date_key`, `pickup_hour_key`, `pickup_zone_key`, `dropoff_zone_key`, `payment_type_key`, `rate_code_key`, `vendor_key`.
- **Medidas:** `passenger_count`, `trip_distance`, `trip_duration_min`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee`, `cbd_congestion_fee`, `total_amount`.
- Puedes conservar `tpep_pickup_datetime` y `tpep_dropoff_datetime` para trazabilidad.
- **No** copies atributos descriptivos (borough, nombre de zona, descripción de pago) a la tabla de hechos: para eso están las dimensiones.

### Nulos

- Los viajes Flex Fare (`payment_type = 0`) llegan sin `RatecodeID`. Su `rate_code_key` debe apuntar a `-1`, y lo mismo para cualquier otra llave sin valor.
- Flex Fare también llega sin `passenger_count`, `congestion_surcharge` ni `airport_fee`, y `cbd_congestion_fee` solo existe desde enero de 2025. Decide si esas medidas quedan en `NULL` o en `0` y **justifícalo**: ¿cómo cambia `AVG(congestion_surcharge)` con cada opción?

### Conciliación (obligatoria)

Al final del notebook, una celda que verifique y **falle** (con `assert` o `raise`) si algo no cuadra:

1. `COUNT(*)` de la tabla de hechos = `COUNT(*)` de silver
2. `SUM(total_amount)` de la tabla de hechos = `SUM(total_amount)` de silver
3. Cero llaves huérfanas: cada llave foránea existe en su dimensión
4. Cero llaves duplicadas en cada dimensión

**Pregunta:** ¿por qué la conciliación debe hacer fallar el notebook en lugar de solo mostrar los números? Piensa en el notebook corriendo dentro de un workflow a las 3 a. m.

---

## Parte 3: Constraints, vistas y permisos

### 3.1 Constraints

- `PRIMARY KEY` en cada dimensión y `FOREIGN KEY` de la tabla de hechos hacia cada dimensión.
- Verifica que quedaron registrados (en Catalog Explorer o en `information_schema.table_constraints`).

**Pregunta:** en Databricks estas constraints son **informativas**: no se hacen cumplir al escribir. Entonces, ¿para qué sirven? ¿Qué cambia si las declaras con `RELY`, y qué riesgo asumes?

### 3.2 Vistas de consumo

Crea al menos dos vistas para Finanzas que unan la estrella y expongan nombres legibles:

- `gold_taxi_dm_v_revenue_mensual`: año, mes, borough de pickup, viajes, revenue, propinas y recargos
- `gold_taxi_dm_v_propinas_tarjeta`: propina promedio por borough, franja horaria y fin de semana, solo donde `tip_is_recorded`

### 3.3 Permisos

En parejas: dale a tu compañero `USE SCHEMA` sobre tu schema y `SELECT` **solo sobre las vistas**. Tu compañero debe demostrar que:

- puede consultar las vistas, y
- **no** puede consultar `fact_trips` ni `silver_yellow_trips_enriched`.

Entrega la salida de `SHOW GRANTS` y el error que recibió tu compañero.

**Pregunta:** ¿por qué tu compañero lee la vista aunque no tenga `SELECT` sobre las tablas que la vista consulta?

---

## Parte 4: Preguntas de negocio

Respóndelas **solo con el mart** (vistas o joins sobre la estrella), sin tocar silver:

1. Revenue mensual por borough de pickup y `service_zone`. ¿Qué participación tienen los aeropuertos?
2. Ticket promedio de los viajes que terminan en un aeropuerto, en días festivos frente a días normales.
3. De los viajes con tarifa JFK (`RatecodeID = 2`), ¿qué porcentaje empieza o termina en JFK Airport? ¿Qué explica el resto?
4. Propina promedio con tarjeta por `day_part` y fin de semana. ¿Cuándo se deja más propina?
5. Mezcla de formas de pago por mes: ¿qué participación tienen tarjeta, efectivo y Flex Fare?
6. Peso de los recargos (`congestion_surcharge` + `airport_fee` + `cbd_congestion_fee`) sobre `total_amount` por borough de pickup.

---

## Parte 5: Rendimiento y diseño físico

1. Escribe dos versiones de esta consulta: una sobre `T_SILVER_TRIPS_ENRICHED` y otra sobre la estrella. Ambas deben devolver el mismo resultado.
   > Top 20 pares zona de origen → zona de destino con más viajes, excluyendo las zonas 264 y 265 en ambos extremos. Por cada par: viajes, distancia promedio, `total_amount` promedio y duración promedio.
2. Compara ambas versiones: tiempo de ejecución y plan (`EXPLAIN` y la pestaña SQL / DataFrame de la Spark UI). ¿Dónde aparecen los broadcast joins?
3. Elige el diseño físico de `fact_trips` (particionado, `ZORDER` o liquid clustering con `CLUSTER BY`) y justifícalo con los filtros que más usan las preguntas de la Parte 4. Ojo: liquid clustering no se combina con particionado ni con `ZORDER`.

**Preguntas:**
- ¿La estrella fue más rápida que la tabla ancha de silver? Si no, ¿por qué? ¿Qué ganas con el mart aunque no gane en velocidad?
- ¿Cuándo seguirías creando una tabla agregada (por ejemplo, demanda por zona × fecha × hora que se consulta cientos de veces al día) aunque tengas el mart?

---

## Parte 6: Orquestación

En una celda `%md`, dibuja el DAG de tareas que agregarías a `07-workflow/resources/nyctaxi_job.yml` para el mart: qué depende de qué, qué corre en paralelo y dónde va la conciliación. No hace falta modificar el bundle.

---

## Entregables

1. Uno o varios notebooks `.py` (formato Databricks). Sugerencia: uno para dimensiones, uno para la tabla de hechos con su conciliación y uno para constraints, vistas y permisos.
2. La sección de diseño de la Parte 0.
3. Cada sección con encabezado `%md`, la transformación y una celda de verificación.
4. Respuestas a todas las preguntas en celdas `%md`.
5. Evidencia de permisos (Parte 3.3) y el DAG (Parte 6).

## Pistas

- Todas las dimensiones son pequeñas: usa `F.broadcast()` al unirlas con los viajes.
- Une las dimensiones con `left join` y completa la llave con `coalesce(..., -1)`. Un `inner join` descartaría en silencio los viajes sin correspondencia y la conciliación fallaría.
- Las columnas de una `PRIMARY KEY` deben ser `NOT NULL`: decláralo al crear la tabla o con `ALTER TABLE ... ALTER COLUMN ... SET NOT NULL`.
- La FK debe referenciar una tabla que ya tenga su PK: crea las dimensiones primero.
- Una llave huérfana se encuentra con un `left_anti` join de la tabla de hechos contra la dimensión.
- Revisa el linaje en Catalog Explorer: debe verse silver → fact → vistas.

