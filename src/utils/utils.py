"""Utilidades del Job 1: configuración, variables de entorno y fechas.

PROYECTO        : Pipeline ETL en GCP - Capa Bronce
NOMBRE          : utils.py
ARCHIVOS DESTINO: No aplica
ARCHIVOS FUENTES: config/parameters.json, variables de entorno
OBJETIVO        : Centralizar la lectura de configuración y parámetros.
TIPO            : Módulo de utilidades
OBSERVACION     : En local las variables vienen del .env; en Cloud Run,
                  API_CONFIG viene de Secret Manager y FECHA_DATOS la envía
                  Airflow.
SCHEDULER       : No aplica
VERSION         : 1.5
DESARROLLADOR   : Jorge
PROVEEDOR       : No aplica
FECHA           : 2026-11-06
DESCRIPCION     : Funciones de apoyo usadas por main.py y las clases.
"""

# seccion Imports
import json
import logging
import os
import re
from datetime import date
from pathlib import Path

RUTA_PARAMETROS = Path(__file__).resolve().parent.parent / "config"
PATRON_FECHA = re.compile(r"\d{4}-\d{2}-\d{2}")
# Carpeta ejecucion=, por ejemplo 20261103T112030Z
FORMATO_EJECUCION = "%Y%m%dT%H%M%SZ"
# Fechas del manifest y la tabla de control, por ejemplo 2026-11-03T11:20:30Z
FORMATO_UTC = "%Y-%m-%dT%H:%M:%SZ"


# seccion Funciones
def configurarLogging() -> None:
    """Configura el logging del job con nivel INFO.

    En Cloud Run (existe CLOUD_RUN_JOB, que Cloud Run define solo) usa
    StructuredLogHandler de google-cloud-logging: cada log sale como
    JSON con su severity (INFO, WARNING, ERROR), el archivo y la línea,
    y los campos de extra={"json_fields": {...}}. En local escribe texto
    legible en la consola.
    """
    if os.environ.get("CLOUD_RUN_JOB"):
        from google.cloud.logging.handlers import StructuredLogHandler

        manejador = StructuredLogHandler()
        # Solo el texto: el nivel ya va en severity y el logger en labels
        manejador.setFormatter(logging.Formatter("%(message)s"))
    else:
        manejador = logging.StreamHandler()
        # Ejemplo: 2026-11-04 17:58:01 [INFO   ] __main__: Iniciando...
        manejador.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)-7s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
    logging.basicConfig(level=logging.INFO, handlers=[manejador], force=True)


def getConfig(nombreArchivo: str) -> dict:
    """Lee el archivo de parámetros de la carpeta config.

    :param nombreArchivo: nombre del archivo JSON dentro de config
    :type nombreArchivo: str
    :return: parámetros del job
    :rtype: dict
    """
    with open(RUTA_PARAMETROS / nombreArchivo, encoding="utf-8") as archivo:
        return json.load(archivo)


def getVariableEntorno(nombre: str) -> str:
    """Lee una variable de entorno obligatoria.

    :param nombre: nombre de la variable
    :type nombre: str
    :return: valor de la variable
    :rtype: str
    :raises ValueError: si la variable no existe o está vacía
    """
    valor = os.environ.get(nombre, "").strip()
    if not valor:
        raise ValueError(
            f"Falta la variable {nombre}. En local agrégala al .env; "
            "en Cloud Run es una variable del job."
        )
    return valor


def validarFecha(fecha: str) -> str:
    """Valida que la fecha tenga exactamente el formato YYYY-MM-DD.

    :param fecha: fecha a validar
    :type fecha: str
    :return: la misma fecha
    :rtype: str
    :raises ValueError: si no es YYYY-MM-DD o no es una fecha real
    """
    if not PATRON_FECHA.fullmatch(fecha or ""):
        raise ValueError(
            f"FECHA_DATOS debe tener formato YYYY-MM-DD: {fecha!r}"
        )
    # Rechaza fechas que no existen, como 2026-02-30
    date.fromisoformat(fecha)
    return fecha


def getFechaDatos() -> str:
    """Obtiene y valida la variable de entorno FECHA_DATOS.

    Es el día al que pertenecen los datos: en la corrida diaria, Airflow
    envía el día anterior (D-1); en un reproceso, el día a reprocesar.

    :return: fecha de datos en formato YYYY-MM-DD
    :rtype: str
    :raises ValueError: si FECHA_DATOS no existe o no es YYYY-MM-DD
    """
    fechaDatos = os.environ.get("FECHA_DATOS")
    if not fechaDatos:
        raise ValueError(
            "Falta FECHA_DATOS (YYYY-MM-DD). En local agrégala al .env; "
            "en Cloud Run la envía Airflow."
        )
    return validarFecha(fechaDatos)


def getContextoEjecucion() -> dict:
    """Lee las variables que cambian en cada ejecución.

    FECHA_DATOS: obligatoria, YYYY-MM-DD (ver getFechaDatos).
    ENDPOINTS: lista separada por comas; vacía = todos los endpoints.
    FORZAR: "true" para volver a extraer aunque ya exista una ejecución
    OK; cualquier otro valor o vacío es false.
    AIRFLOW_DAG_ID, AIRFLOW_TASK_ID, AIRFLOW_RUN_ID, INTENTO: los envía
    Airflow.
    CLOUD_RUN_EXECUTION: lo define Cloud Run en cada ejecución.

    :return: contexto de la ejecución
    :rtype: dict
    :raises ValueError: si FECHA_DATOS no es válida o INTENTO no es un
        número
    """
    fechaDatos = getFechaDatos()
    intento = os.environ.get("INTENTO", "1")
    if not intento.isdigit():
        raise ValueError(f"INTENTO debe ser un número: {intento!r}")
    endpoints = [
        e.strip()
        for e in os.environ.get("ENDPOINTS", "").split(",")
        if e.strip()
    ]
    return {
        "fechaDatos": fechaDatos,
        "endpoints": endpoints,
        "forzar": os.environ.get("FORZAR", "").strip().lower() == "true",
        "dagId": os.environ.get("AIRFLOW_DAG_ID", "manual"),
        "taskId": os.environ.get("AIRFLOW_TASK_ID", "manual"),
        "airflowRunId": os.environ.get("AIRFLOW_RUN_ID", "manual"),
        "intento": int(intento),
        "cloudRunExecution": os.environ.get("CLOUD_RUN_EXECUTION", "local"),
    }
