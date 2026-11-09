"""Pruebas de ClassExtractApi, sin llamar a GCP ni a la API."""

# seccion Imports
import logging
from unittest.mock import MagicMock

import pytest

from tests.classes.apoyo import crearExtractor
from utils.utils import getConfig


# seccion Funciones
@pytest.mark.parametrize(
    "tipo, header, valor",
    [("bearer", "Authorization", "Bearer tk"), ("apiKey", "X-API-Key", "ak")],
)
def testAuthPorHeader(tipo, header, valor):
    """Bearer y apiKey se envían como header."""
    parametrosApi = getConfig("parameters.json")
    parametrosApi["auth"]["tipo"] = tipo
    assert crearExtractor(parametrosApi).sesion.headers[header] == valor


def testAuthBasic():
    """Basic envía usuario y contraseña."""
    parametrosApi = getConfig("parameters.json")
    parametrosApi["auth"]["tipo"] = "basic"
    assert crearExtractor(parametrosApi).sesion.auth == ("usuario", "clave")


def testAuthTipoInvalido():
    """Un tipo de auth desconocido falla al crear la clase."""
    parametrosApi = getConfig("parameters.json")
    parametrosApi["auth"]["tipo"] = "oauth"
    with pytest.raises(ValueError, match="no válido"):
        crearExtractor(parametrosApi)


@pytest.mark.parametrize(
    "tipo, apiConfig",
    [
        ("basic", {"apiUrl": "x", "username": "u"}),
        ("bearer", {"apiUrl": "x"}),
        ("apiKey", {"apiUrl": "x", "token": "tk"}),
    ],
)
def testAuthSinClaves(tipo, apiConfig):
    """API_CONFIG sin las claves del tipo de auth falla."""
    parametrosApi = getConfig("parameters.json")
    parametrosApi["auth"]["tipo"] = tipo
    with pytest.raises(ValueError, match="no tiene los campos"):
        crearExtractor(parametrosApi, apiConfig=apiConfig)


def testLogUrlCompleta(caplog):
    """El log muestra la URL base, la ruta y los query params."""
    extractor = crearExtractor()
    extractor.sesion.get = MagicMock()
    with caplog.at_level(logging.INFO):
        extractor.llamarApi("/ventas", {"desde": "2026-11-02", "pais": "PE"})
    url = "https://api.ejemplo.dev/v2/ventas?desde=2026-11-02&pais=PE"
    assert f"GET {url}" in caplog.text
    extractor.sesion.get.assert_called_once_with(url, timeout=60)


def testEndpointDesconocido():
    """ENDPOINTS con un nombre que no existe falla antes de extraer."""
    with pytest.raises(ValueError):
        crearExtractor(endpoints=["nada"]).ejecutar("2026-11-02")
