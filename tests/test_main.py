"""Pruebas de main.py: el condicional de TABLA_LOG."""

# seccion Imports
import json
from unittest.mock import MagicMock

import pytest

import main


# seccion Funciones
@pytest.fixture
def entorno(monkeypatch):
    """Variables mínimas para main() y un CEA simulado.

    :return: el CEA y el CLI simulados
    :rtype: tuple
    """
    monkeypatch.setattr(main, "load_dotenv", lambda: None)
    monkeypatch.setenv("FECHA_DATOS", "2026-11-02")
    monkeypatch.setenv("BUCKET", "bucket-prueba")
    monkeypatch.setenv("API_CONFIG", json.dumps({"apiUrl": "https://x"}))
    cea = MagicMock()
    cli = MagicMock()
    monkeypatch.setattr(main, "CEA", cea)
    monkeypatch.setattr(main, "CLI", cli)
    return cea, cli


def testSinTablaLogNoRegistra(entorno, monkeypatch):
    """TABLA_LOG vacía: no se crea ClassLogIngesta."""
    cea, cli = entorno
    monkeypatch.setenv("TABLA_LOG", "")
    main.main()
    cli.assert_not_called()
    assert cea.call_args.kwargs["logIngesta"] is None
    cea.return_value.ejecutar.assert_called_once_with("2026-11-02")


def testTablaLogNoDefinida(entorno, monkeypatch):
    """Sin la variable TABLA_LOG tampoco se registra."""
    cea, cli = entorno
    monkeypatch.delenv("TABLA_LOG", raising=False)
    main.main()
    cli.assert_not_called()
    assert cea.call_args.kwargs["logIngesta"] is None


def testConTablaLogRegistra(entorno, monkeypatch):
    """Con TABLA_LOG se crea ClassLogIngesta con esa tabla."""
    cea, cli = entorno
    monkeypatch.setenv("TABLA_LOG", "p.ext_comex.ctl_log_ingesta")
    main.main()
    cli.assert_called_once_with("p.ext_comex.ctl_log_ingesta")
    assert cea.call_args.kwargs["logIngesta"] is cli.return_value
