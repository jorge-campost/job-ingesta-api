# Pipeline ETL en GCP

**Aplicativo:** job-extract (Job 1)

**Capa de Ingesta:** Bronce (crudo en GCS)

## Índice

1. [Introducción General](#introducción-general)
2. [Fuentes a Ingestar](#fuentes-a-ingestar)
3. [Detalles de implementación](#detalles-de-implementación)
4. [Trabajar en local](#trabajar-en-local)
5. [Calidad: linters y pruebas](#calidad-linters-y-pruebas)
6. [Build y deploy](#build-y-deploy)

## Introducción General

Job 1 del pipeline. Es un Cloud Run Job que llama a la API y guarda cada respuesta sin cambios en el bucket de crudos, una página por archivo, con un `_manifest.json` por endpoint y ejecución. Airflow 3 lo ejecuta todos los días con `FECHA_DATOS` = día anterior (D-1) y también permite reprocesar un día pasando esa fecha. El Job 2 carga ese crudo en BigQuery.

Cada ejecución queda en su propia carpeta, así que un reintento o reproceso nunca pisa lo anterior:

```
gs://dev-stg-gst-comex-001/
  frankfurter/                                   <- fuente (parameters.json: fuente)
    rates/                                       <- carpeta del endpoint (parameters.json: carpeta)
      fecha_datos=2026-11-02/
        tipo_carga=incremental/                  <- modo del endpoint
          ejecucion=20261103T110512Z/            <- intento que falló
            _manifest.json                       <- estado = FALLIDO
          ejecucion=20261103T112030Z/            <- reintento
            frankfurter_rates_inc_20261102_p00001.json
            _manifest.json                       <- estado = OK
    currencies/
      fecha_datos=2026-11-02/                    <- en un snapshot, fecha de corte
        tipo_carga=snapshot/                     <- catálogo: siempre completo
          ejecucion=20261103T112031Z/
            frankfurter_currencies_snap_20261102_p00001.json
            _manifest.json
```

`fecha_datos` es el día al que pertenecen los datos (D-1), no el día en que corre el job; ese queda en `ejecucion=` (UTC) y en el manifest.

## Fuentes a Ingestar

| Endpoint | Modo | Parámetros | Destino en el bucket |
|---|---|---|---|
| `/rates` | incremental | `date=FECHA_DATOS` | `frankfurter/rates/fecha_datos=FECHA_DATOS/tipo_carga=incremental/ejecucion=.../` |
| `/currencies` | snapshot | ninguno | `frankfurter/currencies/fecha_datos=FECHA_DATOS/tipo_carga=snapshot/ejecucion=.../` |

La fuente del laboratorio es Frankfurter v2, que no pagina ni tiene `modifiedFrom`. Las pruebas unitarias son generales (auth, URL, variables, contexto, tabla de control) y no dependen de los endpoints de Frankfurter, para poder copiarlas a otra API sin cambios.

## Detalles de implementación

```
job-extract/
├── src/
│   ├── main.py              # main(): lee la configuración e instancia ClassExtractApi y ClassLogIngesta
│   ├── classes/classes.py   # ClassExtractApi y ClassLogIngesta
│   ├── utils/utils.py       # getConfig, getFechaDatos, getContextoEjecucion, getVariableEntorno...
│   └── config/parameters.json
├── tests/                   # misma estructura que src/
│   ├── test_main.py         # main(): variables obligatorias
│   ├── classes/
│   │   ├── apoyo.py                    # API_CONFIG y extractor con GCS simulado
│   │   ├── test_class_extract_api.py   # ClassExtractApi (sin GCP ni API)
│   │   └── test_class_log_ingesta.py   # ClassLogIngesta (sin BigQuery)
│   └── utils/
│       └── test_utils.py    # utils/utils.py
├── .flake8, .pre-commit-config.yaml, pytest.ini
├── Dockerfile, cloudbuild.yaml, requirements.txt
└── .env.example
```

### Qué hace por cada endpoint

1. Toma el tipo del `modo` del endpoint en `parameters.json` (`incremental` o `snapshot`) y arma los query params de su lista `params`.
2. Si ya existe una ejecución con manifest `OK` para ese día y tipo, lo **omite** (salvo `FORZAR=true`). Así un reintento de Airflow solo repite los endpoints que fallaron.
3. Crea la carpeta `ejecucion=<UTC>/` y registra `INICIADO` en la tabla de control.
4. Descarga las páginas y guarda cada una apenas llega. Con paginación, lee `totalCount` y `totalPages` de la página 1 y sigue hasta `totalPages`.
5. Valida: páginas guardadas = `totalPages` y suma de `items` = `totalCount`. Si no cuadra, o los totales cambian entre páginas, el estado es `INCOMPLETO`.
6. Siempre (también si falla) escribe `_manifest.json` y registra el estado final: `OK`, `FALLIDO` o `INCOMPLETO`.

Un endpoint que falla no detiene a los demás. Al final, si alguno quedó `FALLIDO` o `INCOMPLETO`, el job termina con error para que Airflow lo reintente (máximo 3 veces).

### Manifest

Uno por endpoint y ejecución. Guarda fuente, endpoint, tipo, `carpeta`, `fecha_datos`, ejecución, estado, inicio, fin, duración, datos de Airflow (DAG, tarea, run_id, intento), ejecución de Cloud Run, ruta, query params y `params_dinamicos` (los que vinieron de variables de entorno; sin credenciales), `page_size`, `total_count`, `total_pages`, registros descargados, la lista de archivos (nombre, página, registros, bytes, md5) y el error.

### Tabla de control

El job inserta una fila por evento en `ctl_log_ingesta` (DDL en `sql/ddl_ctl_log_ingesta.sql`). Solo inserta, nunca actualiza. Si BigQuery falla, deja un warning y sigue: la fuente de verdad es el manifest. El service account del job necesita `roles/bigquery.dataEditor` sobre el dataset `ext_comex`.

### Logs

El job usa el `logging` estándar de Python (`log.info`, `log.warning`, `log.exception`). `configurarLogging()` elige el handler según dónde corre:

| Dónde | Handler | Salida |
| --- | --- | --- |
| Cloud Run (existe `CLOUD_RUN_JOB`, que Cloud Run define solo) | `StructuredLogHandler` de `google-cloud-logging` | Una línea JSON por log con `severity`, `message`, archivo y línea, y los campos de `json_fields` |
| Local | `StreamHandler` | Texto: `INFO classes.classes ...` |

Un endpoint `OK` se registra como INFO; uno `FALLIDO` o `INCOMPLETO`, como ERROR, igual que el resumen final si algo falló. El traceback de un error queda dentro del mismo log. Para agregar campos filtrables a un log:

```python
log.info("Página guardada", extra={"json_fields": {"endpoint": "rates", "pagina": 3}})
```

Filtros útiles en **Logging** → **Logs Explorer**:

```
resource.type="cloud_run_job"
resource.labels.job_name="job-extract"
severity>=WARNING
jsonPayload.endpoint="rates"
jsonPayload.fecha_datos="2026-11-02"
```

### Usar el job con otra API

Lo que cambia de una API a otra está en `src/config/parameters.json` y en el secreto. El código no se toca.

**Autenticación** (`auth.tipo`) y claves que debe tener el secreto `API_CONFIG`:

| `auth.tipo` | Cómo se envía | Claves en `API_CONFIG` |
|---|---|---|
| `none` | sin credenciales | `apiUrl` |
| `basic` | usuario y contraseña | `apiUrl`, `username`, `password` |
| `bearer` | header `Authorization: Bearer <token>` | `apiUrl`, `token` |
| `apiKey` | header indicado en `auth.headerApiKey` (por defecto `X-API-Key`) | `apiUrl`, `apiKey` |

**Endpoints:** la clave es el nombre que se usa en `ENDPOINTS`. Cada uno tiene `carpeta` (carpeta en el bucket), `ruta`, `modo` (`incremental` o `snapshot`, define `tipo_carga=`), `paginado` (true/false) y `params`. Todo lo que está en `params` se envía siempre, así que es obligatorio:

- Valor fijo: se envía tal cual, por ejemplo `{"nombre": "continent", "valor": "europe"}`.
- Valor `{env:NOMBRE}`: toma la variable de entorno `NOMBRE` (del `.env` en local, de Airflow en Cloud Run). Si falta, el endpoint falla con un error que la nombra. El job no calcula fechas: `MODIFIED_FROM`/`MODIFIED_TO` las manda Airflow.

Solo `FECHA_DATOS` define carpetas; las demás variables no crean niveles en el bucket. Las credenciales van en `API_CONFIG`, nunca en `{env:...}`.

Para tener el mismo endpoint como incremental y snapshot, decláralo dos veces con la misma `carpeta` (por ejemplo `rates` y `rates_snapshot`): quedan en la misma carpeta con distinto `tipo_carga=`, y cada DAG elige cuál corre con `ENDPOINTS`.

**Paginación** (sección `paginacion`, común a la API): nombre de los parámetros de página y tamaño, `tamanoPagina`, `paginaInicial`, campos `items`, `totalCount` y `totalPages`, y `maxPaginas` como tope de seguridad. Los parámetros de página se agregan solos a los endpoints con `paginado: true`.

Ejemplo para un endpoint tipo CargoProduce (los nombres de parámetros son a confirmar con la API real):

```json
"fuente": "cargoproduce",
"endpoints": {
    "embarques": {
        "carpeta": "embarques",
        "ruta": "/embarques",
        "modo": "incremental",
        "paginado": true,
        "params": [
            {"nombre": "modifiedFrom", "valor": "{env:MODIFIED_FROM}"},
            {"nombre": "modifiedTo", "valor": "{env:MODIFIED_TO}"}
        ]
    }
}
```

**Destino:** `fuente` es la primera carpeta dentro del bucket; las plantillas de carpeta y archivo están en `paths`. El bucket se define con la variable `BUCKET`.

### De dónde salen las variables

| Variable | En tu PC | En Cloud Run |
|---|---|---|
| `API_CONFIG` | archivo `.env` | secreto `api-config` de Secret Manager (`--set-secrets` en `cloudbuild.yaml`) |
| `BUCKET`, `TABLA_LOG` | archivo `.env` | variables del job (`--set-env-vars` en `cloudbuild.yaml`). Obligatorias |
| `FECHA_DATOS` | archivo `.env` | la pasa Airflow en cada ejecución (D-1). Obligatoria y exactamente `YYYY-MM-DD` |
| `ENDPOINTS`, `FORZAR` | archivo `.env` | opcionales; cada DAG pasa `ENDPOINTS` con lo que corre; `FORZAR` en un reproceso |
| Cada `{env:NOMBRE}` de `params` (p. ej. `MODIFIED_FROM`) | archivo `.env` | la pasa Airflow; obligatoria si el endpoint la usa |
| `AIRFLOW_DAG_ID`, `AIRFLOW_TASK_ID`, `AIRFLOW_RUN_ID`, `INTENTO` | archivo `.env` (o vacías) | las pasa Airflow (`{{ dag.dag_id }}`, `{{ ti.task_id }}`, `{{ run_id }}`, `{{ ti.try_number }}`) |
| `CLOUD_RUN_EXECUTION` | no aplica (`local`) | la define Cloud Run sola |

`main.py` llama a `load_dotenv()`, que busca un `.env` desde `src/` hacia arriba. Si no lo encuentra no hace nada, y nunca pisa una variable ya definida. El `.env` está en `.gitignore` y `.dockerignore`.

## Trabajar en local

PowerShell, cuenta DEV:

```powershell
cd C:\Users\jcamp\OneDrive\Documents\gcp\job-extract
.\.venv\Scripts\Activate.ps1

# Librerías del job (van a requirements.txt)
pip install requests google-cloud-storage google-cloud-bigquery google-cloud-logging python-dotenv
# Herramientas de desarrollo (cada desarrollador las instala en su venv; no van a requirements.txt)
pip install black flake8 pydocstyle pytest pre-commit sphinx sphinx_markdown_builder

copy .env.example .env        # solo si aún no existe
gcloud auth application-default login
python src\main.py
```

`requirements.txt` lleva solo las librerías del job, con versión fija. Ya trae estas cinco; si actualizas alguna, toma la versión de `pip freeze`:

```
requests==<versión>
google-cloud-storage==<versión>
google-cloud-bigquery==<versión>
google-cloud-logging==<versión>
python-dotenv==<versión>
```

## Calidad: linters y pruebas

```powershell
pre-commit install             # una vez: activa los chequeos en cada git commit
pre-commit run --all-files     # black, flake8 y pydocstyle
pytest                         # pruebas en tests/
```

La salida de estos dos comandos es la evidencia para la revisión entre pares.

## Build y deploy

El `Dockerfile` copia `src/` y ejecuta `main.py` desde ahí. Con el trigger `build-job-extract` de Cloud Build, cada push a `main` hace:

1. `docker build` con el tag del commit (`SHORT_SHA`).
2. `docker push` a `us-central1-docker.pkg.dev/jclab-dev-dlk-services/dev-ctn-arg-comex-001/job-ingesta-api` (imagen con el mismo nombre que el repo de GitHub).
3. `gcloud run jobs deploy job-extract` en `jclab-dev-brz-tmp`, con `API_CONFIG` desde el secreto `api-config:latest`, `BUCKET` y `TABLA_LOG` como variables y `--max-retries=0`: los reintentos los hace solo Airflow, para que cada intento quede visible y no se multipliquen las llamadas a la API.

Para probar la imagen en local:

```powershell
docker build -t job-ingesta-api .
docker run --rm --env-file .env -v "$env:APPDATA\gcloud:/root/.config/gcloud:ro" job-ingesta-api
```

Para ejecutarlo a mano en GCP: `gcloud run jobs execute job-extract --project=jclab-dev-brz-tmp --region=us-central1 --update-env-vars=FECHA_DATOS=2026-11-02 --wait`.
