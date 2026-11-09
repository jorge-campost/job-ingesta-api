"""Objetos de apoyo para las pruebas de classes/classes.py."""

# seccion Imports
from unittest.mock import MagicMock

from classes.classes import ClassExtractApi
from utils.utils import getConfig

API_CONFIG = {
    "apiUrl": "https://api.ejemplo.dev/v2/",
    "username": "usuario",
    "password": "clave",
    "token": "tk",
    "apiKey": "ak",
}


# seccion Funciones
def contextoEjecucion(**cambios) -> dict:
    """Contexto de ejecución por defecto, con cambios opcionales."""
    base = {
        "fechaDatos": "2026-11-02",
        "endpoints": [],
        "forzar": False,
        "dagId": "dag_prueba",
        "taskId": "tarea_prueba",
        "airflowRunId": "manual__1",
        "intento": 1,
        "cloudRunExecution": "local",
    }
    base.update(cambios)
    return base


def crearExtractor(
    parametrosApi: dict = None, logIngesta=None, apiConfig=None, **ctx
) -> ClassExtractApi:
    """Crea un ClassExtractApi con GCS y la tabla de control simulados."""
    return ClassExtractApi(
        apiConfig=apiConfig or API_CONFIG,
        parametrosApi=parametrosApi or getConfig("parameters.json"),
        nombreBucket="bucket-prueba",
        contextoEjecucion=contextoEjecucion(**ctx),
        clienteStorage=MagicMock(),
        logIngesta=logIngesta or MagicMock(),
    )
