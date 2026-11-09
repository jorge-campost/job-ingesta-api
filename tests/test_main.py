"""Pruebas de main.py: variables obligatorias y armado de las clases."""

# seccion Imports
import json
import logging
from unittest.mock import MagicMock

import pytest

import main


# seccion Funciones
@pytest.fixture
def entorno(monkeypatch):
    """Variables mínimas para main() y las clases simuladas.

    :return: ClassExtractApi y ClassLogIngesta simuladas
    :rtype: tuple
    """
    monkeypatch.setattr(main, "load_dotenv", lambda: None)
    # configurarLogging reemplaza los handlers y le quitaría el suyo a caplog
    monkeypatch.setattr(main, "configurarLogging", lambda: None)
    monkeypatch.setenv("FECHA_DATOS", "2026-11-02")
    monkeypatch.setenv("BUCKET", "bucket-prueba")
    monkeypatch.setenv("TABLA_LOG", "p.ext_comex.ctl_log_ingesta")
    monkeypatch.setenv("API_CONFIG", json.dumps({"apiUrl": "https://x"}))
    extractApi = MagicMock()
    logIngesta = MagicMock()
    monkeypatch.setattr(main, "ClassExtractApi", extractApi)
    monkeypatch.setattr(main, "ClassLogIngesta", logIngesta)
    return extractApi, logIngesta


def testConTablaLogRegistra(entorno):
    """Instancia ClassLogIngesta con TABLA_LOG y ejecuta con la fecha."""
    extractApi, logIngesta = entorno
    main.main()
    logIngesta.assert_called_once_with("p.ext_comex.ctl_log_ingesta")
    assert extractApi.call_args.kwargs["logIngesta"] is logIngesta.return_value
    extractApi.return_value.ejecutar.assert_called_once_with("2026-11-02")


@pytest.mark.parametrize("variable", ["BUCKET", "TABLA_LOG", "API_CONFIG"])
def testVariableObligatoria(entorno, monkeypatch, variable):
    """Sin BUCKET, TABLA_LOG o API_CONFIG falla antes de extraer."""
    extractApi, _ = entorno
    monkeypatch.setenv(variable, "")
    with pytest.raises(ValueError, match=variable):
        main.main()
    extractApi.assert_not_called()


def testLogApiUrlSinCredenciales(entorno, monkeypatch, caplog):
    """Registra la URL de la API pero nunca usuario ni contraseña."""
    monkeypatch.setenv(
        "API_CONFIG",
        json.dumps(
            {"apiUrl": "https://x", "username": "u", "password": "secreta"}
        ),
    )
    with caplog.at_level(logging.INFO):
        main.main()
    assert "Iniciando ingesta de API https://x" in caplog.text
    assert "secreta" not in caplog.text
