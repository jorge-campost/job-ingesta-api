"""Pruebas de ClassLogIngesta, sin llamar a BigQuery."""

# seccion Imports
import logging
from unittest.mock import MagicMock

from classes.classes import ClassLogIngesta as CLI

TABLA = "jclab-dev-brz-tmp.ext_comex.ctl_log_ingesta"
FILA = {"etapa": "EXTRACCION", "estado": "OK", "endpoint": "rates"}


# seccion Funciones
def testRegistraUnaFila():
    """Inserta la fila tal cual en la tabla indicada."""
    cliente = MagicMock()
    cliente.insert_rows_json.return_value = []
    CLI(TABLA, clienteBigquery=cliente).registrar(FILA)
    cliente.insert_rows_json.assert_called_once_with(TABLA, [FILA])


def testSoloInsertaNoCreaTabla():
    """No crea ni modifica la tabla: eso lo hace el DDL."""
    cliente = MagicMock()
    cliente.insert_rows_json.return_value = []
    CLI(TABLA, clienteBigquery=cliente).registrar(FILA)
    cliente.create_table.assert_not_called()
    cliente.query.assert_not_called()


def testErroresDeFilaSoloAvisan(caplog):
    """Si BigQuery rechaza la fila, deja un warning y no lanza error."""
    cliente = MagicMock()
    cliente.insert_rows_json.return_value = [
        {"index": 0, "errors": [{"reason": "invalid"}]}
    ]
    with caplog.at_level(logging.WARNING):
        CLI(TABLA, clienteBigquery=cliente).registrar(FILA)
    assert "No se pudo registrar" in caplog.text


def testExcepcionNoDetieneElJob(caplog):
    """Tabla inexistente o sin permisos: warning y sigue."""
    cliente = MagicMock()
    cliente.insert_rows_json.side_effect = Exception("404 Not found: Table")
    with caplog.at_level(logging.WARNING):
        CLI(TABLA, clienteBigquery=cliente).registrar(FILA)
    assert "404 Not found" in caplog.text
