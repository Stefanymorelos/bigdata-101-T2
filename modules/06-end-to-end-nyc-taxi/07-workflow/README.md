# 07 - Workflow (Asset Bundle)

Orquestación del pipeline completo con **Databricks Asset Bundles (DABs)** — Infrastructure as Code para Databricks. El pipeline corre como un Databricks Job con dependencias entre tasks.

## DAG del pipeline

Estos son exactamente los `task_key` definidos en `resources/nyctaxi_job.yml`:

```
bronze_trips          bronze_zones
     │                     │
     ▼                     │
silver_clean               │
     │                     │
     └──────────┬──────────┘
                ▼
         silver_enriched
                │
     ┌──────────┼──────────────┬────────────────────┐
     ▼          ▼              ▼                    ▼
gold_revenue  gold_daily  gold_hourly        gold_ml_features
_by_zone      _metrics    _demand                   │
                                                    ▼
                                                  train
                                                    │
                                                    ▼
                                                 register
                                                    │
                                                    ▼
                                             batch_inference
```

`bronze_trips` y `bronze_zones` arrancan en paralelo: no dependen de nada, porque el landing ya está poblado. No hay task de descarga — el landing es de solo lectura y lo prepara el profesor con `00-setup/_admin_download_landing.py`.


## Comandos

Instalar la CLI de Databricks: https://docs.databricks.com/dev-tools/cli/install.html

```bash
# Configurar perfil (una vez)
databricks configure

# Validar el bundle
databricks bundle validate --target dev

# Desplegar al workspace de dev
databricks bundle deploy --target dev

# Correr el pipeline
databricks bundle run nyctaxi_pipeline --target dev

# Promover a prod (equivalente a despliegue productivo)
databricks bundle deploy --target prod
```

## Estructura

- `databricks.yml` — Definición del bundle (jobs, clusters, targets)
- `resources/nyctaxi_job.yml` — Definición del job con sus tasks y dependencias
