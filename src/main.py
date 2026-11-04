"""Punto de entrada del Job 1 (Cloud Run Job job-extract).

PROYECTO: Pipeline ETL en GCP - Capa Bronce
NOMBRE: main.py
ARCHIVOS DESTINO: gs://<BUCKET>/<fuente>/<endpoint>/tipo=<tipo>/
    fecha_datos=<FECHA_DATOS>/ejecucion=<YYYYMMDDTHHMMSSZ>/
ARCHIVOS FUENTES: API Frankfurter v2 (/rates, /currencies)
OBJETIVO: Guardar en GCS la respuesta cruda de la API para una
    FECHA_DATOS, con un manifest por endpoint y ejecución.
TIPO: Cloud Run Job
OBSERVACION: Variables de entorno: API_CONFIG (Secret Manager en Cloud
    Run, .env en local), BUCKET, FECHA_DATOS (obligatoria, YYYY-MM-DD)
    y opcionales TABLA_LOG, TIPO_CARGA, ENDPOINTS, FORZAR,
    AIRFLOW_DAG_ID, AIRFLOW_TASK_ID, AIRFLOW_RUN_ID e INTENTO.
    La auth, los endpoints y sus parámetros están en parameters.json.
SCHEDULER: Airflow 3, diario, FECHA_DATOS = día anterior (D-1);
    reproceso manual con el parámetro fecha del DAG
VERSION: 1.3
DESARROLLADOR: Jorge
PROVEEDOR: Frankfurter (laboratorio)
FECHA: 2026-11-04
DESCRIPCION: Lee la configuración e instancia ClassExtractApi (CEA) y,
    si TABLA_LOG está definida, ClassLogIngesta (CLI).
"""

# seccion Imports
import logging
import os

from dotenv import load_dotenv

from classes.classes import ClassExtractApi as CEA
from classes.classes import ClassLogIngesta as CLI
from utils.utils import (
    configurarLogging,
    getApiConfig,
    getConfig,
    getContexto,
    getFechaDatos,
)

log = logging.getLogger(__name__)


# seccion Funciones
def main() -> None:
    """Ejecuta la extracción de la API hacia el bucket de crudos."""
    # El .env solo existe en local y no pisa variables ya definidas
    load_dotenv()
    configurarLogging()
    fechaDatos = getFechaDatos()
    parametros = getConfig()
    tablaLog = os.environ.get("TABLA_LOG")
    if not tablaLog:
        log.warning("TABLA_LOG no está definida: no se registra el log")
    apiConfig = getApiConfig(parametros["auth"]["tipo"])
    # Prueba: muestra que el secreto llegó. Solo la URL, nunca credenciales
    log.info(
        "Iniciando ingesta de API %s para %s",
        apiConfig["apiUrl"],
        fechaDatos,
    )
    extractor = CEA(
        apiConfig=apiConfig,
        parametros=parametros,
        nombreBucket=os.environ["BUCKET"],
        contexto=getContexto(),
        logIngesta=CLI(tablaLog) if tablaLog else None,
    )
    extractor.ejecutar(fechaDatos)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Deja el error con nivel ERROR en Cloud Logging y sale con código 1
        # para que Cloud Run marque la ejecución como fallida y Airflow
        # reintente
        log.exception("El job terminó con error")
        raise SystemExit(1)
