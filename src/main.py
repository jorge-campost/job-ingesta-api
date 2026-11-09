"""Punto de entrada del Job 1 (Cloud Run Job job-extract).

PROYECTO        : Pipeline ETL en GCP - Capa Bronce
NOMBRE          : main.py
ARCHIVOS DESTINO: gs://<BUCKET>/<fuente>/<carpeta>/fecha_datos=<FECHA_DATOS>/
                  tipo_carga=<modo>/ejecucion=<YYYYMMDDTHHMMSSZ>/
ARCHIVOS FUENTES: API Frankfurter v2 (/rates, /currencies)
OBJETIVO        : Guardar en GCS la respuesta cruda de la API para una
                  FECHA_DATOS, con un manifest por endpoint y ejecución.
TIPO            : Cloud Run Job
OBSERVACION     : Variables de entorno: API_CONFIG (Secret Manager en Cloud
                  Run, .env en local), BUCKET, FECHA_DATOS (obligatoria,
                  YYYY-MM-DD), TABLA_LOG y opcionales ENDPOINTS, FORZAR,
                  AIRFLOW_DAG_ID, AIRFLOW_TASK_ID, AIRFLOW_RUN_ID e INTENTO,
                  más cada {env:NOMBRE} usado en parameters.json. La auth,
                  los endpoints, su modo y sus parámetros están en
                  parameters.json.
SCHEDULER       : Airflow 3, diario, FECHA_DATOS = día anterior (D-1);
                  reproceso manual con el parámetro fecha_datos del DAG
VERSION         : 1.5
DESARROLLADOR   : Jorge
PROVEEDOR       : Frankfurter (laboratorio)
FECHA           : 2026-11-06
DESCRIPCION     : Lee la configuración e instancia ClassLogIngesta y
                  ClassExtractApi.
"""

import json
import logging

from dotenv import load_dotenv

from classes.classes import ClassExtractApi, ClassLogIngesta
from utils.utils import (
    configurarLogging,
    getConfig,
    getContextoEjecucion,
    getVariableEntorno,
)

log = logging.getLogger(__name__)


def main() -> None:
    """Ejecuta la extracción de la API hacia el bucket de crudos."""
    # El .env solo existe en local y no pisa variables ya definidas
    load_dotenv()
    configurarLogging()
    contextoEjecucion = getContextoEjecucion()
    parametrosApi = getConfig("parameters.json")
    nombreBucket = getVariableEntorno("BUCKET")
    tablaLog = getVariableEntorno("TABLA_LOG")
    apiConfig = json.loads(getVariableEntorno("API_CONFIG"))
    log.info(
        "Iniciando ingesta de API %s para %s",
        apiConfig["apiUrl"],
        contextoEjecucion["fechaDatos"],
    )
    extractor = ClassExtractApi(
        apiConfig=apiConfig,
        parametrosApi=parametrosApi,
        nombreBucket=nombreBucket,
        contextoEjecucion=contextoEjecucion,
        logIngesta=ClassLogIngesta(tablaLog),
    )
    extractor.ejecutar(contextoEjecucion["fechaDatos"])


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Deja el error con nivel ERROR en Cloud Logging y sale con código 1
        # para que Cloud Run marque la ejecución como fallida y Airflow
        # reintente
        log.exception("El job terminó con error")
        raise SystemExit(1)
