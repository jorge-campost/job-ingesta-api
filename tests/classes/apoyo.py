"""Datos y objetos de apoyo para las pruebas de classes/classes.py."""

# seccion Imports
import json
from unittest.mock import MagicMock

from classes.classes import ClassExtractApi as CEA
from utils.utils import getConfig

API_CONFIG = {
    "apiUrl": "https://api.ejemplo.dev/v2/",
    "username": "usuario",
    "password": "clave",
    "token": "tk",
    "apiKey": "ak",
}
FECHA = "2026-11-02"
# Respuestas con la forma real de Frankfurter v2 (ver job-load)
RATES = [
    {"date": FECHA, "base": "USD", "quote": "EUR", "rate": 0.9312},
    {"date": FECHA, "base": "USD", "quote": "PEN", "rate": 3.7605},
    {"date": FECHA, "base": "USD", "quote": "BRL", "rate": 5.4321},
]
CURRENCIES = [
    {
        "iso_code": "EUR",
        "iso_numeric": 978,
        "name": "Euro",
        "symbol": "€",
        "start_date": "1999-01-04",
        "end_date": None,
    },
    {
        "iso_code": "PEN",
        "iso_numeric": 604,
        "name": "Sol",
        "symbol": "S/",
        "start_date": "1999-01-04",
        "end_date": None,
    },
]


# seccion Funciones
class BucketFalso:
    """Bucket de GCS en memoria: guarda el texto de cada objeto."""

    def __init__(self) -> None:
        """Crea el bucket vacío."""
        self.name = "bucket-prueba"
        self.objetos = {}

    def blob(self, nombre: str):
        """Devuelve un blob que escribe y lee en el diccionario."""
        blob = MagicMock()
        blob.name = nombre
        blob.upload_from_string.side_effect = (
            lambda contenido, content_type: self.objetos.__setitem__(
                nombre, contenido.decode("utf-8")
            )
        )
        blob.download_as_text.side_effect = lambda: self.objetos[nombre]
        return blob

    def list_blobs(self, prefix: str):
        """Lista los objetos que empiezan con el prefijo."""
        return [self.blob(n) for n in self.objetos if n.startswith(prefix)]

    def leer(self, nombre: str):
        """Devuelve el JSON guardado en un objeto."""
        return json.loads(self.objetos[nombre])

    def manifests(self) -> list:
        """Devuelve los manifests guardados, en orden de nombre."""
        return [
            self.leer(n)
            for n in sorted(self.objetos)
            if n.endswith("/_manifest.json")
        ]


def contexto(**cambios) -> dict:
    """Contexto de ejecución por defecto, con cambios opcionales."""
    base = {
        "tipoCarga": "incremental",
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


def crearExtractor(parametros: dict = None, logIngesta=None, **ctx) -> CEA:
    """Crea un CEA con un bucket en memoria."""
    bucket = BucketFalso()
    clienteStorage = MagicMock()
    clienteStorage.bucket.return_value = bucket
    return CEA(
        apiConfig=API_CONFIG,
        parametros=parametros or getConfig(),
        nombreBucket=bucket.name,
        contexto=contexto(**ctx),
        clienteStorage=clienteStorage,
        logIngesta=logIngesta,
    )


def parametrosPaginados() -> dict:
    """parameters.json con rates paginado.

    Frankfurter no pagina: se simula envolviendo los registros de rates
    en items, totalCount y totalPages, como lo hará la API real.
    """
    parametros = getConfig()
    rates = parametros["endpoints"]["rates"]
    rates["paginado"] = True
    rates["paramsIncrementales"] += [
        {"nombre": "modifiedFrom", "valor": "{modifiedFrom}"},
        {"nombre": "modifiedTo", "valor": "{modifiedTo}"},
    ]
    parametros["endpoints"] = {"rates": rates}
    parametros["paginacion"]["tamanoPagina"] = 2
    return parametros


def paginas(*listas, total=None) -> list:
    """Envuelve listas de rates en items, totalCount y totalPages."""
    totalCount = total if total is not None else sum(map(len, listas))
    return [
        {"items": items, "totalCount": totalCount, "totalPages": len(listas)}
        for items in listas
    ]
