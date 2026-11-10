# Pipeline ETL en GCP

**Aplicativo:** job-extract (Job 1)

**Capa de Ingesta:** Bronce (crudo en GCS)

## Índice

1. [Introducción General](#introducción-general)
2. [Fuentes a Ingestar](#fuentes-a-ingestar)
3. [Detalles de implementación](#detalles-de-implementación)
4. [Trabajar en local](#trabajar-en-local)
5. [Build y deploy](#build-y-deploy)

## Introducción General

Cloud Run Job que llama a la API y guarda cada respuesta sin cambios en el bucket de crudos (un JSON por página y un `_manifest.json` por endpoint y ejecución). Airflow lo ejecuta a diario con `FECHA_DATOS` = D-1 o con la fecha de un reproceso. El Job 2 carga ese crudo en BigQuery.

```
gs://<BUCKET>/<fuente>/<carpeta>/fecha_datos=YYYY-MM-DD/tipo_carga=<modo>/ejecucion=<YYYYMMDDTHHMMSSZ>/
    <fuente>_<carpeta>_<inc|snap>_<YYYYMMDD>_p00001.json
    _manifest.json
```

Cada ejecución tiene su carpeta `ejecucion=` (UTC), así que un reintento o reproceso nunca pisa lo anterior. En un snapshot, `fecha_datos` es la fecha de corte.

## Fuentes a Ingestar

| Endpoint | Modo | Parámetros |
|---|---|---|
| `/rates` | incremental | `date={env:FECHA_DATOS}` |
| `/currencies` | snapshot | ninguno |

Fuente de laboratorio: Frankfurter v2 (sin auth ni paginación).

## Detalles de implementación

```
src/
├── main.py                # lee variables y config, crea las clases y ejecuta
├── classes/classes.py     # ClassExtractApi y ClassLogIngesta
├── utils/utils.py         # getConfig, getContextoEjecucion, getVariableEntorno, logging
└── config/parameters.json # todo lo propio de la API
tests/                     # pruebas generales, no dependen de la API
docs/                      # guía para otra API y parameters.example.json
requirements-dev.txt       # requirements.txt + herramientas de desarrollo
```

### Flujo por endpoint

1. Si `ENDPOINTS` trae un nombre inexistente o falta alguna variable `{env:...}`, el job falla antes de llamar a la API.
2. Si ya hay un manifest `OK` para la misma carpeta, fecha y tipo, el endpoint se omite (salvo `FORZAR=true`): un reintento de Airflow solo repite lo que falló.
3. Registra `INICIADO`, descarga y guarda página por página (la URL completa de cada GET queda en el log).
4. Con paginación valida páginas = `totalPages` y registros = `totalCount`; si no cuadra, `INCOMPLETO`.
5. Siempre escribe `_manifest.json` y registra el estado final (`OK`, `FALLIDO`, `INCOMPLETO`).

Un endpoint fallido no detiene a los demás; al final el job sale con error si alguno quedó `FALLIDO` o `INCOMPLETO`.

**Manifest:** datos de la ejecución, de Airflow y de Cloud Run, request (`params` y `params_dinamicos`, sin credenciales), totales, archivos (página, registros, bytes, md5) y error.

**Tabla de control** (`control_ingesta`, DDL en `sql/`): una fila por evento, solo inserciones. El job no crea la tabla. Si el insert falla, deja un warning y sigue.

**Logs:** en Cloud Run (`CLOUD_RUN_JOB` definida) salen como JSON estructurado, con los campos de `extra={"json_fields": {...}}` filtrables en Logs Explorer (`jsonPayload.endpoint`, `jsonPayload.fecha_datos`); en local, como texto.

### Configuración (`parameters.json`)

Para otra API se copia el repo y se cambia solo este archivo, el secreto y las variables del build y del DAG: ver [docs/reutilizar-con-otra-api.md](docs/reutilizar-con-otra-api.md). [`docs/parameters.example.json`](docs/parameters.example.json) es un ejemplo funcional (Frankfurter).

- `auth.tipo` y claves que debe traer `API_CONFIG`: `none` (`apiUrl`), `basic` (`+username`, `password`), `bearer` (`+token`), `apiKey` (`+apiKey`, header en `auth.headerApiKey`).
- `endpoints`: la clave es el nombre usado en `ENDPOINTS`. Campos `carpeta`, `ruta`, `modo` (`incremental`/`snapshot`), `paginado` y `params`. Cada valor de `params` es fijo o `{env:NOMBRE}`; todos son obligatorios. Mismo endpoint en ambos modos: dos entradas con la misma `carpeta`.
- `paginacion`: nombres de los parámetros de página y tamaño, campos `items`/`totalCount`/`totalPages`, `tamanoPagina`, `paginaInicial` y `maxPaginas`.
- `paths`, `filesExt`, `others`: plantillas de carpeta y archivo, tipo de contenido, timeout y reintentos HTTP.

No soportado sin cambiar código: métodos distintos de GET, respuestas no JSON, paginación por cursor u offset, variables en la ruta y OAuth.

### Variables de entorno

| Variable | Origen en Cloud Run | Obligatoria |
|---|---|---|
| `API_CONFIG` | Secret Manager (`--set-secrets`) | sí |
| `BUCKET`, `TABLA_LOG` | `--set-env-vars` en `cloudbuild.yaml` | sí |
| `FECHA_DATOS` (`YYYY-MM-DD`) y cada `{env:NOMBRE}` | Airflow | sí |
| `ENDPOINTS`, `FORZAR` | Airflow o ejecución manual | no |
| `AIRFLOW_DAG_ID`, `AIRFLOW_TASK_ID`, `AIRFLOW_RUN_ID`, `INTENTO` | Airflow | no (por defecto `manual` y `1`) |

En local se leen del `.env` (ver `.env.example`).

## Trabajar en local

```powershell
pip install -r requirements-dev.txt   # incluye requirements.txt
copy .env.example .env
gcloud auth application-default login
python src\main.py

pre-commit install            # una vez: black, flake8 y pydocstyle en cada commit
pre-commit run --all-files    # ejecuta los checks sobre todo el repo
pytest
```

## Build y deploy

El trigger de Cloud Build, en cada push a `main`, construye la imagen (tag `SHORT_SHA`), la sube a Artifact Registry y despliega el Cloud Run Job con `--max-retries=0` (los reintentos los hace Airflow). Los valores por ambiente están en `substitutions` de `cloudbuild.yaml`.

Ejecución manual:

```
gcloud run jobs execute job-extract --project=jclab-dev-brz-tmp --region=us-central1 --update-env-vars=FECHA_DATOS=2026-11-02 --wait
```
