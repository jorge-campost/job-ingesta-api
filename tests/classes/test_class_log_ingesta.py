"""Pruebas de ClassLogIngesta, sin llamar a BigQuery."""

# seccion Imports
import logging
from unittest.mock import MagicMock

from classes.classes import ClassLogIngesta

TABLA = "jclab-dev-brz-tmp.ext_comex.ctl_log_ingesta"


def manifestDePrueba() -> dict:
    """Manifest de una ejecución OK con 3 registros, de un endpoint ficticio.

    :return: manifest con los campos que lee ClassLogIngesta
    :rtype: dict
    """
    return {
        "fuente": "api_prueba",
        "endpoint": "ventas",
        "tipo": "incremental",
        "fecha_datos": "2026-11-02",
        "request": {"params_dinamicos": {"desde": "2026-11-02"}},
        "ejecucion": "20261103T112030Z",
        "airflow": {
            "dag_id": "dag_prueba",
            "task_id": "tarea_prueba",
            "run_id": "manual__1",
            "intento": 1,
        },
        "cloud_run_execution": "local",
        "total_count": 3,
        "total_pages": 1,
        "registros_descargados": 3,
        "ruta_gcs": "gs://bucket-prueba/api_prueba/ventas/",
        "error": None,
        "duracion_seg": 1.5,
    }


# seccion Funciones
def testRegistraUnaFila():
    """Convierte el manifest en una fila y la inserta en la tabla."""
    cliente = MagicMock()
    cliente.insert_rows_json.return_value = []
    ClassLogIngesta(TABLA, clienteBigquery=cliente).registrar(
        manifestDePrueba(), "OK"
    )
    tabla, filas = cliente.insert_rows_json.call_args.args
    assert tabla == TABLA
    fila = filas[0]
    assert fila["estado"] == "OK"
    assert fila["etapa"] == "EXTRACCION"
    assert fila["endpoint"] == "ventas"
    assert fila["params_dinamicos"] == '{"desde": "2026-11-02"}'
    assert fila["registros"] == 3
    assert fila["task_id"] == "tarea_prueba"
    assert fila["airflow_run_id"] == "manual__1"


def testErroresDeFilaSoloAvisan(caplog):
    """Si BigQuery rechaza la fila, deja un warning y no lanza error."""
    cliente = MagicMock()
    cliente.insert_rows_json.return_value = [
        {"index": 0, "errors": [{"reason": "invalid"}]}
    ]
    with caplog.at_level(logging.WARNING):
        ClassLogIngesta(TABLA, clienteBigquery=cliente).registrar(
            manifestDePrueba(), "OK"
        )
    assert "No se pudo registrar" in caplog.text


def testExcepcionNoDetieneElJob(caplog):
    """Tabla inexistente o sin permisos: warning y sigue."""
    cliente = MagicMock()
    cliente.insert_rows_json.side_effect = Exception("404 Not found: Table")
    with caplog.at_level(logging.WARNING):
        ClassLogIngesta(TABLA, clienteBigquery=cliente).registrar(
            manifestDePrueba(), "OK"
        )
    assert "404 Not found" in caplog.text
