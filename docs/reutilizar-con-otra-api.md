# Reutilizar el job con otra API

Cada API nueva tiene su propio repo de GitHub, copia de este. El código Python, los tests y el `Dockerfile` no se tocan; solo cambia lo siguiente.

Antes de empezar, confirma que la API cabe en lo que soporta el job: solo GET, respuestas JSON, paginación por número de página (o sin paginación), rutas sin variables y auth `none`, `basic`, `bearer` o `apiKey` (sin OAuth). Si no, hay que cambiar código.

## 1. Repo

- Crea el repo nuevo copiando este. Su nombre será el de la imagen (`_NOMBRE_IMAGEN`).

## 2. `src/config/parameters.json`

Parte de [`parameters.example.json`](parameters.example.json), que es la configuración funcional de Frankfurter.

| Campo | Qué poner |
|---|---|
| `fuente` | Nombre corto de la API. Es la primera carpeta en el bucket y el valor de `nom_fuente`. |
| `auth.tipo` | `none`, `basic`, `bearer` o `apiKey`. Con `apiKey`, `auth.headerApiKey` es el nombre del header. |
| `paginacion` | Nombres de los parámetros de página y tamaño, de los campos `items`/`totalCount`/`totalPages` de la respuesta, `tamanoPagina`, `paginaInicial` y `maxPaginas`. Si ningún endpoint pagina, se deja como está. |
| `endpoints` | Una entrada por endpoint. La clave es el nombre que va en `ENDPOINTS`. Campos: `carpeta`, `ruta`, `modo` (`incremental`/`snapshot`), `paginado` y `params`. |
| `endpoints.*.params` | Cada valor es fijo o `{env:NOMBRE}`. Las variables `{env:...}` las tiene que mandar Airflow. Nunca credenciales aquí. |
| `others` | Timeout, reintentos y códigos HTTP que se reintentan, según lo que tolere la API. |

`paths` y `filesExt` normalmente no cambian.

## 3. Secreto en Secret Manager

- Crea el secreto (por ejemplo `api-config-<fuente>`) con un JSON: `apiUrl` más las claves de `auth.tipo`: `basic` → `username`, `password`; `bearer` → `token`; `apiKey` → `apiKey`.
- La SA del job necesita `roles/secretmanager.secretAccessor` sobre ese secreto.

## 4. BigQuery

- Dataset `ext__<fuente>`.
- Tabla de control: ejecuta `sql/ddl_control_ingesta.sql` cambiando el dataset en el `CREATE TABLE` (y en `sql/consultas_control_ingesta.sql`).
- `roles/bigquery.dataEditor` sobre el dataset para la SA del job y el grupo DEV.

## 5. `cloudbuild.yaml` (substitutions)

| Variable | Cambia a |
|---|---|
| `_NOMBRE_IMAGEN` | Nombre del repo nuevo |
| `_JOB` | Nombre del Cloud Run Job nuevo, por ejemplo `job-extract-<fuente>` |
| `_SECRETO` | Secreto del paso 3 |
| `_DATASET` | `ext__<fuente>` |
| `_SA_JOB`, `_BUCKET` | Solo si la API usa otra SA u otro bucket |

`_REGION`, `_REPO_AR`, `_PROYECTO_JOB`, `_PROYECTO_DATASET` y `_TABLA_CONTROL` se mantienen por ambiente.

Luego crea el trigger de Cloud Build para el repo nuevo (push a `main`).

## 6. DAG de Airflow

En el repo de DAGs, un DAG (o tarea) nuevo con:
- `job_name` = `_JOB`.
- `ENDPOINTS` con las claves de `parameters.json`.
- Una variable por cada `{env:NOMBRE}` usado en `params`, además de `FECHA_DATOS`.
- El `Param` `forzar` y el resto de overrides igual que en el DAG actual.

## 7. Local

- `.env.example`: `API_CONFIG`, `TABLA_LOG` y los `{env:...}` de ejemplo de la nueva API.
- `README.md`: tabla de *Fuentes a Ingestar* y el comando de ejecución manual.

## 8. Verificar

1. `pytest` (los tests no dependen de la API).
2. `python src\main.py` en local con un solo endpoint (`ENDPOINTS=...`).
3. Revisa en el bucket los JSON y el `_manifest.json`, y en `control_ingesta` el evento `OK`.
4. Después del deploy, `gcloud run jobs execute <_JOB> --update-env-vars=FECHA_DATOS=...` y luego el DAG.
