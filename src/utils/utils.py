"""Utilidades del Job 1: configuración, variables de entorno y fechas.

PROYECTO: Pipeline ETL en GCP - Capa Bronce
NOMBRE: utils.py
ARCHIVOS DESTINO: No aplica
ARCHIVOS FUENTES: config/parameters.json, variables de entorno
OBJETIVO: Centralizar la lectura de configuración y parámetros.
TIPO: Módulo de utilidades
OBSERVACION: En local las variables vienen del .env; en Cloud Run,
    API_CONFIG viene de Secret Manager y FECHA_DATOS la envía Airflow.
SCHEDULER: No aplica
VERSION: 1.3
DESARROLLADOR: Jorge
PROVEEDOR: No aplica
FECHA: 2026-11-04
DESCRIPCION: Funciones de apoyo usadas por main.py y las clases.
"""

# seccion Imports
import json
import logging
import os
import re
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

RUTA_PARAMETROS = Path(__file__).resolve().parent.parent / "config"
PATRON_FECHA = re.compile(r"\d{4}-\d{2}-\d{2}")
TIPOS_CARGA = ("incremental", "snapshot")
VALORES_VERDADEROS = ("true", "1", "si", "sí", "yes")
# Claves que debe tener el secreto API_CONFIG según el tipo de auth
CAMPOS_POR_AUTH = {
    "none": {"apiUrl"},
    "basic": {"apiUrl", "username", "password"},
    "bearer": {"apiUrl", "token"},
    "apiKey": {"apiUrl", "apiKey"},
}


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
        manejador.setFormatter(
            logging.Formatter("%(levelname)s %(name)s %(message)s")
        )
    logging.basicConfig(level=logging.INFO, handlers=[manejador], force=True)


def getConfig(nombreArchivo: str = "parameters.json") -> dict:
    """Lee el archivo de parámetros de la carpeta config.

    :param nombreArchivo: nombre del archivo JSON dentro de config
    :type nombreArchivo: str
    :return: parámetros del job
    :rtype: dict
    """
    with open(RUTA_PARAMETROS / nombreArchivo, encoding="utf-8") as archivo:
        return json.load(archivo)


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


def leerBooleano(nombre: str) -> bool:
    """Lee una variable de entorno como verdadero o falso.

    :param nombre: nombre de la variable
    :type nombre: str
    :return: True si vale true, 1, si o yes
    :rtype: bool
    """
    return os.environ.get(nombre, "").strip().lower() in VALORES_VERDADEROS


def getContexto() -> dict:
    """Lee las variables de la ejecución que no son la fecha.

    TIPO_CARGA: incremental (por defecto) o snapshot.
    ENDPOINTS: lista separada por comas; vacía = todos los endpoints.
    FORZAR: true para volver a extraer aunque ya exista una ejecución OK.
    AIRFLOW_DAG_ID, AIRFLOW_TASK_ID, AIRFLOW_RUN_ID, INTENTO: los envía
    Airflow.
    CLOUD_RUN_EXECUTION: lo define Cloud Run en cada ejecución.

    :return: contexto de la ejecución
    :rtype: dict
    :raises ValueError: si TIPO_CARGA o INTENTO no son válidos
    """
    tipoCarga = os.environ.get("TIPO_CARGA", "incremental").strip().lower()
    if tipoCarga not in TIPOS_CARGA:
        raise ValueError(
            f"TIPO_CARGA {tipoCarga!r} no válido. Usa {list(TIPOS_CARGA)}"
        )
    intento = os.environ.get("INTENTO", "1")
    if not intento.isdigit():
        raise ValueError(f"INTENTO debe ser un número: {intento!r}")
    endpoints = [
        e.strip()
        for e in os.environ.get("ENDPOINTS", "").split(",")
        if e.strip()
    ]
    return {
        "tipoCarga": tipoCarga,
        "endpoints": endpoints,
        "forzar": leerBooleano("FORZAR"),
        "dagId": os.environ.get("AIRFLOW_DAG_ID", "manual"),
        "taskId": os.environ.get("AIRFLOW_TASK_ID", "manual"),
        "airflowRunId": os.environ.get("AIRFLOW_RUN_ID", "manual"),
        "intento": int(intento),
        "cloudRunExecution": os.environ.get("CLOUD_RUN_EXECUTION", "local"),
    }


def calcularVentana(fecha: str, formato: str) -> tuple:
    """Calcula la ventana incremental de un día: [fecha, fecha + 1).

    :param fecha: fecha de datos (YYYY-MM-DD)
    :type fecha: str
    :param formato: formato strftime que espera la API
    :type formato: str
    :return: (modifiedFrom, modifiedTo) como texto
    :rtype: tuple
    """
    desde = datetime.combine(date.fromisoformat(fecha), time.min)
    hasta = desde + timedelta(days=1)
    return desde.strftime(formato), hasta.strftime(formato)


def ahoraUtc() -> datetime:
    """Devuelve la fecha y hora actual en UTC.

    :return: momento actual con zona UTC
    :rtype: datetime
    """
    return datetime.now(timezone.utc)


def generarEjecucion(momento: datetime) -> str:
    """Arma el identificador de la carpeta ejecucion=.

    :param momento: inicio de la ejecución, en UTC
    :type momento: datetime
    :return: timestamp compacto, por ejemplo 20261103T112030Z
    :rtype: str
    """
    return momento.strftime("%Y%m%dT%H%M%SZ")


def formatearUtc(momento: datetime) -> str:
    """Convierte un momento UTC a texto ISO 8601 con Z.

    :param momento: fecha y hora con zona UTC
    :type momento: datetime
    :return: por ejemplo 2026-11-03T11:20:30Z
    :rtype: str
    """
    return momento.strftime("%Y-%m-%dT%H:%M:%SZ")


def getApiConfig(tipoAuth: str) -> dict:
    """Lee el JSON de API_CONFIG y valida sus claves según la auth.

    :param tipoAuth: tipo de autenticación (none, basic, bearer, apiKey)
    :type tipoAuth: str
    :return: configuración de la API (apiUrl y credenciales)
    :rtype: dict
    :raises KeyError: si la variable API_CONFIG no existe
    :raises ValueError: si el tipo de auth no existe o faltan claves
    """
    if tipoAuth not in CAMPOS_POR_AUTH:
        raise ValueError(
            f"auth.tipo {tipoAuth!r} no válido. "
            f"Usa uno de {sorted(CAMPOS_POR_AUTH)}"
        )
    apiConfig = json.loads(os.environ["API_CONFIG"])
    faltantes = CAMPOS_POR_AUTH[tipoAuth] - apiConfig.keys()
    if faltantes:
        raise ValueError(f"API_CONFIG no tiene los campos {sorted(faltantes)}")
    return apiConfig
