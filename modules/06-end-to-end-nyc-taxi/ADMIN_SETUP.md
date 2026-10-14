# Guía de Setup para el Profesor/Admin

Esta guía es SOLO para el profesor o administrador del workspace. Los estudiantes nunca hacen estos pasos. Si los completas bien una vez, cada estudiante solo debe poner su `SCHEMA` en `config.py` y correr los notebooks.

## Qué vas a configurar

1. Una **storage account ADLS Gen2** con un container compartido
2. Un **Access Connector for Azure Databricks** que actúa como managed identity
3. Permisos del connector sobre el container
4. Un **metastore** de Unity Catalog (si no existe) asignado al workspace
5. Una **storage credential** y una **external location** en Unity Catalog, más **el catálogo compartido del curso**
6. Un **grupo de estudiantes** con los privilegios correctos sobre ese catálogo
7. El **landing compartido** poblado con los Parquet de la TLC
8. Un **SQL Warehouse** compartido (opcional pero recomendado para el dashboard)
9. Los **Git folders** de cada estudiante

## El modelo de aislamiento

Hay **un solo catálogo para todo el curso** (`20080_databricks` en la iteración actual; si lo cambias, avísales a los estudiantes, porque va en `00-setup/config.py`). El aislamiento ocurre un nivel más abajo: **cada estudiante tiene su propio schema** dentro de ese catálogo, con su `nombre_apellido` en snake_case. Ese schema lo **creas tú** y se lo asignas como dueño: los estudiantes no tienen `CREATE SCHEMA` sobre el catálogo, solo `USE CATALOG`. El modelo de ML también va en ese mismo schema, así que no hace falta un schema `_ml` aparte.

Dentro de su schema cada estudiante escribe todas sus capas como **tablas administradas** de Unity Catalog, y usa su Volume `datos` (el de módulos anteriores) solo para checkpoints de streaming. Como es dueño de su schema, no necesita grants adicionales para `CREATE TABLE` ni `CREATE VOLUME`; lo que sí necesita el catálogo es una managed location (propia o heredada del metastore), porque ahí es donde UC guarda los datos de las tablas administradas. El landing con los datos crudos vive en el schema `default` del mismo catálogo y es de **solo lectura** para ellos.

> El catálogo empieza por dígito, así que toda referencia en SQL necesita backticks: `` `20080_databricks` ``.

## Prerrequisitos

- Ser **Azure Subscription Owner** (o tener `Contributor` + `User Access Administrator`) sobre la suscripción donde va el storage
- Ser **Databricks Account Admin** en el account console (portal `accounts.azuredatabricks.net`). Ser Subscription Owner NO te hace automáticamente account admin.
- Azure CLI instalada localmente (opcional pero ayuda)

## Paso 1 — Crear la storage account y el container

Desde Azure Portal o con Azure CLI:

```bash
# Variables
RG="rg-upb-bigdata"
LOCATION="eastus2"
STORAGE="upbbigdatastorage"            # debe ser único globalmente, solo minúsculas y números
CONTAINER="nyctaxi-lake"

# Resource group
az group create --name $RG --location $LOCATION

# Storage account con hierarchical namespace (ADLS Gen2)
az storage account create \
  --name $STORAGE \
  --resource-group $RG \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Container
az storage container create \
  --name $CONTAINER \
  --account-name $STORAGE \
  --auth-mode login
```

Apunta: `<container>=nyctaxi-lake` y `<storage-account>=upbbigdatastorage`. Son valores tuyos, para la external location del Paso 5; los estudiantes nunca los ven, porque acceden todo por Volumes de Unity Catalog.

## Paso 2 — Crear el Access Connector

El Access Connector es el recurso que Databricks usa para autenticarse contra el storage sin secrets.

Desde Portal: **Create a resource** → buscar "Access Connector for Azure Databricks" → crearlo en el mismo resource group.

Con Azure CLI:
```bash
CONNECTOR="upb-databricks-connector"

az databricks access-connector create \
  --name $CONNECTOR \
  --resource-group $RG \
  --location $LOCATION \
  --identity-type SystemAssigned
```

Apunta el **resource ID** del connector:
```
/subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Databricks/accessConnectors/<connector-name>
```

## Paso 3 — Dar permisos al connector sobre el container

El connector tiene una managed identity; hay que darle el rol **Storage Blob Data Contributor** sobre el container.

```bash
# Obtener el principal ID del connector
PRINCIPAL_ID=$(az databricks access-connector show \
  --name $CONNECTOR --resource-group $RG \
  --query identity.principalId -o tsv)

# Scope del container
SCOPE=$(az storage account show \
  --name $STORAGE --resource-group $RG \
  --query id -o tsv)"/blobServices/default/containers/$CONTAINER"

# Asignar rol
az role assignment create \
  --assignee-object-id $PRINCIPAL_ID \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope $SCOPE
```

## Paso 4 — Metastore de Unity Catalog

Si tu workspace ya tiene Unity Catalog habilitado (típico para workspaces creados en los últimos 12-18 meses), salta este paso.

Para verificar, entra al workspace y ve a **Catalog**. Si ves una jerarquía de catálogos, está habilitado.

Si no:
1. Ir a `accounts.azuredatabricks.net` como Account Admin
2. **Data** → **Create metastore**
3. Seleccionar la región del workspace
4. Proveer el connector del paso 2 como storage credential inicial
5. Asignar el metastore al workspace

Referencia: [Tutorial: Unity Catalog metastore admin tasks](https://learn.microsoft.com/en-us/azure/databricks/getting-started/uc-metastore-admin-quickstart).

## Paso 5 — Storage credential, external location y catálogo del curso

Esto se hace **dentro del workspace de Databricks** (no en Azure). La storage credential y la external location son infraestructura **tuya**: sirven al metastore y al catálogo compartido que creas aquí. Los estudiantes nunca las tocan ni las referencian desde los notebooks: todo lo que ellos leen y escriben pasa por Volumes de Unity Catalog (`/Volumes/...`).

### Storage Credential

Catalog Explorer → **External Data** → **Credentials** → **Create credential**:
- **Credential type**: Azure Managed Identity
- **Name**: `nyctaxi_credential`
- **Access connector ID**: pega el resource ID del paso 2

O vía SQL:
```sql
CREATE STORAGE CREDENTIAL IF NOT EXISTS nyctaxi_credential
  WITH AZURE_MANAGED_IDENTITY
  '/subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Databricks/accessConnectors/<connector-name>'
  COMMENT 'Credential para el lakehouse de NYC Taxi';
```

### External Location

Catalog Explorer → **External Data** → **External Locations** → **Create external location**:
- **Name**: `nyctaxi_lake`
- **Storage credential**: `nyctaxi_credential`
- **URL**: `abfss://nyctaxi-lake@upbbigdatastorage.dfs.core.windows.net/`

O vía SQL:
```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS nyctaxi_lake
  URL 'abfss://nyctaxi-lake@upbbigdatastorage.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL nyctaxi_credential)
  COMMENT 'Container ADLS Gen2 para el lakehouse de NYC Taxi';
```

Validar que funciona:
```sql
-- Debe listar (o devolver vacío sin error) el contenido del container
LIST 'abfss://nyctaxi-lake@upbbigdatastorage.dfs.core.windows.net/';
```

### El catálogo compartido y los Volumes del landing

```sql
CREATE CATALOG IF NOT EXISTS `20080_databricks`
  MANAGED LOCATION 'abfss://nyctaxi-lake@upbbigdatastorage.dfs.core.windows.net/'
  COMMENT 'Catálogo compartido del curso de Big Data';

-- El landing vive en el schema default del catálogo
CREATE VOLUME IF NOT EXISTS `20080_databricks`.default.nytaxi_landing
  COMMENT 'Parquet mensuales de NYC TLC — solo lectura para estudiantes';

CREATE VOLUME IF NOT EXISTS `20080_databricks`.default.nytaxi_zones
  COMMENT 'Lookup de zonas de taxi — solo lectura para estudiantes';
```

### Verificar la managed location del catálogo

Los estudiantes escriben todas las capas como **tablas administradas**, así que el catálogo necesita una managed location. Ojo con el `IF NOT EXISTS` de arriba: si el catálogo ya existía de otra iteración del curso **sin** location, esa sentencia no hace nada y no avisa.

```sql
DESCRIBE CATALOG EXTENDED `20080_databricks`;
-- Revisa la fila "Storage Root". Si está vacía y el metastore tampoco tiene
-- storage root, fíjala:
ALTER CATALOG `20080_databricks`
  SET MANAGED LOCATION 'abfss://nyctaxi-lake@upbbigdatastorage.dfs.core.windows.net/';
```

`00-setup/01_create_catalog.py` crea y borra una tabla de prueba (`_setup_probe`) en el schema del estudiante justo después de crearlo. Si falla ahí, es esto.

## Paso 6 — Grupo de estudiantes y permisos

### Crear el grupo en el account console

1. `accounts.azuredatabricks.net` → **User management** → **Groups** → **Add group**
2. Nombre: `bigdata-students`
3. Agregar los usuarios de los estudiantes
4. Asignar el grupo al workspace: **Workspaces** → seleccionar tu workspace → **Permissions** → agregar el grupo con acceso

### Permisos del grupo sobre el catálogo compartido

Los estudiantes **no** necesitan `CREATE CATALOG` ni `CREATE SCHEMA`. Necesitan usar el catálogo del curso, ser dueños de su schema y leer el landing.

En el workspace, abre un notebook SQL y ejecuta:

```sql
-- Usar el catálogo compartido (sin CREATE SCHEMA)
GRANT USE CATALOG ON CATALOG `20080_databricks` TO `bigdata-students`;

-- Un schema por estudiante, creado por ti y con el estudiante como dueño.
-- Repetir por cada estudiante:
CREATE SCHEMA IF NOT EXISTS `20080_databricks`.camilo_soto;
ALTER SCHEMA `20080_databricks`.camilo_soto OWNER TO `camiloandres.soto@upb.edu.co`;

-- Leer el landing de solo lectura
GRANT USE SCHEMA ON SCHEMA `20080_databricks`.default TO `bigdata-students`;
GRANT READ VOLUME ON VOLUME `20080_databricks`.default.nytaxi_landing
  TO `bigdata-students`;
GRANT READ VOLUME ON VOLUME `20080_databricks`.default.nytaxi_zones
  TO `bigdata-students`;

-- Verificar
SHOW GRANTS ON CATALOG `20080_databricks`;
SHOW GRANTS ON VOLUME `20080_databricks`.default.nytaxi_landing;
```

Como dueño de su schema, cada estudiante puede crear ahí sus tablas, su Volume `datos` y su modelo registrado (`CREATE MODEL`) sin más grants. No les des `WRITE VOLUME` sobre el landing: es de solo lectura a propósito.

## Paso 7 — Poblar el landing compartido

Los estudiantes no descargan nada de internet: leen los Parquet ya depositados en los Volumes del landing. Esa descarga la haces tú, una sola vez, corriendo el notebook `00-setup/_admin_download_landing.py` desde el workspace. Está marcado **ADMIN ONLY** y es el único notebook del módulo que escribe en `default.nytaxi_landing` y `default.nytaxi_zones`.

Revisa antes las constantes `TRIPS_DIR` y `ZONES_DIR` de ese notebook: apuntan al catálogo de una iteración anterior del curso, así que si cambiaste de catálogo hay que ajustarlas para que coincidan con `LANDING_TRIPS_PATH` y `LANDING_ZONES_PATH` de `00-setup/config.py`.

Para la cátedra, 6 meses (2024-01 a 2024-06) son suficientes. Verifica el resultado:

```sql
LIST '/Volumes/20080_databricks/default/nytaxi_landing';
LIST '/Volumes/20080_databricks/default/nytaxi_zones';
```

## Paso 8 — Compute (cluster o SQL Warehouse)

Los estudiantes necesitan algo sobre lo cual correr sus notebooks.

### Opción A — Cluster personal (recomendado)

Crear una **Cluster Policy** compartida que limite el tamaño y auto-termine:
- **Policy family**: Personal Compute
- **Max workers**: 2
- **Auto-termination**: 30 min
- **Runtime**: 15.4 LTS o superior (con Photon)
- **Access mode**: Single User

Permitir al grupo usarla:
```sql
-- Desde Databricks CLI o UI
databricks cluster-policies create --json-file policy.json
databricks permissions update cluster-policies <policy-id> \
  --json '{"access_control_list":[{"group_name":"bigdata-students","permission_level":"CAN_USE"}]}'
```

Los estudiantes crean su cluster personal desde esta policy — no pueden saltarse los límites.

### Opción B — SQL Warehouse compartido

Si quieres que todos compartan compute (más barato pero hay colas):
1. **SQL** → **SQL Warehouses** → **Create**
2. **Cluster size**: X-Small o Small
3. **Auto-stop**: 10 min
4. **Permisos**: CAN USE para `bigdata-students`

Para el dashboard final necesitas esto sí o sí.

## Paso 9 — Git y workspace folders

Recomiendo que cada estudiante clone este repo como un **Databricks Repo** en su home:
1. **Workspace** → su carpeta home → **Add** → **Git folder**
2. Pegar la URL del repo
3. Se crea `/Users/<estudiante>/big-data/modules/06-end-to-end-nyc-taxi/...`

Cada estudiante trabaja en su propia copia, edita su `config.py` sin pisar al resto.

## Checklist final de validación

Antes de liberar a los estudiantes, prueba con una cuenta de prueba que esté en `bigdata-students`:

- [ ] Puede hacer login al workspace
- [ ] Ve el catálogo compartido en Catalog Explorer
- [ ] Puede crear un cluster personal desde la policy
- [ ] Puede correr `01_create_catalog.py` sin errores, poniendo un `SCHEMA` único en `config.py`
- [ ] Después de correr el notebook, ve **su schema y su Volume `datos`** en Catalog Explorer
- [ ] Puede leer el landing que tú poblaste con `_admin_download_landing.py` (el notebook lo verifica con un `dbutils.fs.ls` al final)
- [ ] Puede correr todo el pipeline hasta Gold

Si los 7 checks pasan, la clase está lista.

## Costos estimados (referencia)

Por semestre, para un grupo de 20 estudiantes haciendo el pipeline completo:

- **Storage ADLS Gen2**: ~2-5 USD/mes (20 schemas × ~5 GB cada uno, más el landing compartido)
- **Compute**: depende del uso. Clusters personales Standard_DS3_v2 con 2 workers salen a ~0.80 USD/hora/estudiante. Con auto-stop de 30 min y uso de ~4h/semana por estudiante → ~60-80 USD/mes para el grupo.
- **SQL Warehouse X-Small**: ~0.38 DBU/hora. Con auto-stop agresivo → ~20-40 USD/mes.

Total aproximado: **100-150 USD/mes** para el curso completo.

Para ahorrar: usar **serverless SQL warehouses** para el dashboard (pago por segundo en vez de por hora), y pedir a los estudiantes que terminen sus clusters al acabar.

## Troubleshooting común

**`SCHEMA_NOT_FOUND` / `Schema 'xxx' does not exist`**
→ El estudiante no corrió `00-setup/01_create_catalog.py`, o lo corrió con un `SCHEMA` distinto al que tiene ahora en `config.py`. Que revise el bloque editable y lo vuelva a correr.

**`PERMISSION_DENIED: User does not have CREATE SCHEMA on Catalog`**
→ Algún notebook está intentando crear un schema; los estudiantes no tienen ese permiso a propósito. Pasa típicamente con un `CREATE SCHEMA IF NOT EXISTS` sobre un schema que existe pero sobre el que el estudiante no tiene ningún permiso: para él es invisible, así que UC intenta crearlo. Revisa que el schema del estudiante exista y que sea su dueño (Paso 6).

**`01_create_catalog.py` dice "No encuentro el schema"**
→ El schema del estudiante no existe o no es suyo. Créalo y asígnale el dueño (Paso 6), o corrige el `SCHEMA` que puso en `config.py`.

**`TABLE_OR_VIEW_NOT_FOUND` / `Catalog '...' does not exist` al hacer `USE CATALOG`**
→ El nombre del catálogo en `config.py` no es el del semestre, o falta el `GRANT USE CATALOG`. El notebook `01_create_catalog.py` falla a propósito en este caso, con un mensaje que apunta al docente. Ojo: el catálogo empieza por dígito y en SQL necesita backticks.

**`No se pudo crear una tabla administrada` en `01_create_catalog.py`** (o `saveAsTable` falla con un error de storage / `NO_PARENT_EXTERNAL_LOCATION_FOR_PATH` / `METASTORE_DOES_NOT_HAVE_STORAGE_ROOT`)
→ Paso 5: el catálogo no tiene managed location y el metastore tampoco. Revísalo con `DESCRIBE CATALOG EXTENDED` y fíjala con `ALTER CATALOG ... SET MANAGED LOCATION`.

**`PERMISSION_DENIED` leyendo `/Volumes/<catalogo>/default/nytaxi_landing`**
→ Paso 6: faltan el `GRANT USE SCHEMA ON SCHEMA ...default` o los `GRANT READ VOLUME`. Si el path no existe, es el Paso 7: el landing está vacío.

**`AbfsRestOperationException: 403 AuthorizationPermissionMismatch`**
→ Paso 3: el connector no tiene `Storage Blob Data Contributor` sobre el container, o se lo pusiste al resource group en vez de al container.

**Dos estudiantes escribiendo en el mismo schema**
→ Eligieron el mismo `SCHEMA` en `config.py` (típico si alguien dejó el valor por defecto `camilo_soto`). Si el otro estudiante le dio permisos sobre su schema, nada falla: simplemente comparten espacio y se pisan los datos. Revisa con `SHOW SCHEMAS IN` que haya un schema por estudiante y pídeles que usen su `nombre_apellido`.

**El streaming falla leyendo el Volume**
→ Verificar que el cluster use **Runtime 13.3 LTS o superior** y **access mode Single User o Shared** (no No Isolation Shared, que no soporta UC).

**El notebook tarda mucho en la primera corrida**
→ Normal: el cluster tarda ~5 min en arrancar. Con serverless es instantáneo pero cuesta más.
