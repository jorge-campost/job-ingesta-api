"""Pruebas unitarias de utils/utils.py."""

# seccion Imports
import json
from datetime import datetime, timezone

import pytest

from utils.utils import (
    calcularVentana,
    generarEjecucion,
    getApiConfig,
    getConfig,
    getContexto,
    getFechaDatos,
    validarFecha,
)


# seccion Funciones
def testGetConfigTieneSecciones():
    """parameters.json trae las secciones que usa el job."""
    parametros = getConfig()
    for seccion in (
        "fuente",
        "auth",
        "ventana",
        "paginacion",
        "endpoints",
        "paths",
        "filesExt",
        "others",
    ):
        assert seccion in parametros


def testFechaFaltante(monkeypatch):
    """Sin FECHA_DATOS el job falla."""
    monkeypatch.delenv("FECHA_DATOS", raising=False)
    with pytest.raises(ValueError):
        getFechaDatos()


@pytest.mark.parametrize(
    "fecha",
    ["21/10/2026", "20261021", "2026-1-05", "2026-10-21T00:00", "2026-02-30"],
)
def testFechaSoloAceptaYYYYMMDD(fecha):
    """Formatos alternativos o fechas inexistentes fallan."""
    with pytest.raises(ValueError):
        validarFecha(fecha)


def testFechaValida(monkeypatch):
    """Una FECHA_DATOS válida se devuelve igual."""
    monkeypatch.setenv("FECHA_DATOS", "2026-10-21")
    assert getFechaDatos() == "2026-10-21"


def testVentanaCruzaMes():
    """La ventana de un día termina a las 00:00 del día siguiente."""
    assert calcularVentana("2026-10-31", "%Y-%m-%dT%H:%M:%S") == (
        "2026-10-31T00:00:00",
        "2026-11-01T00:00:00",
    )


def testGenerarEjecucion():
    """La ejecución es un timestamp UTC compacto."""
    momento = datetime(2026, 11, 3, 11, 20, 30, tzinfo=timezone.utc)
    assert generarEjecucion(momento) == "20261103T112030Z"


def testContextoPorDefecto(monkeypatch):
    """Sin variables: incremental, todos los endpoints, intento 1."""
    for v in ("TIPO_CARGA", "ENDPOINTS", "FORZAR", "INTENTO"):
        monkeypatch.delenv(v, raising=False)
    ctx = getContexto()
    assert ctx["tipoCarga"] == "incremental"
    assert ctx["endpoints"] == []
    assert ctx["forzar"] is False
    assert ctx["intento"] == 1


def testContextoDesdeAirflow(monkeypatch):
    """Lee las variables que envía Airflow."""
    monkeypatch.setenv("TIPO_CARGA", "Snapshot")
    monkeypatch.setenv("ENDPOINTS", "rates, currencies")
    monkeypatch.setenv("FORZAR", "true")
    monkeypatch.setenv("INTENTO", "3")
    ctx = getContexto()
    assert ctx["tipoCarga"] == "snapshot"
    assert ctx["endpoints"] == ["rates", "currencies"]
    assert ctx["forzar"] is True
    assert ctx["intento"] == 3


def testTipoCargaInvalido(monkeypatch):
    """Un TIPO_CARGA desconocido falla."""
    monkeypatch.setenv("TIPO_CARGA", "full")
    with pytest.raises(ValueError):
        getContexto()


def testApiConfigIncompleta(monkeypatch):
    """API_CONFIG sin apiUrl falla."""
    monkeypatch.setenv("API_CONFIG", json.dumps({"username": "u"}))
    with pytest.raises(ValueError):
        getApiConfig("basic")


def testApiConfigBearerSinToken(monkeypatch):
    """Con auth bearer, API_CONFIG debe traer token."""
    monkeypatch.setenv("API_CONFIG", json.dumps({"apiUrl": "x"}))
    with pytest.raises(ValueError):
        getApiConfig("bearer")


def testAuthTipoInvalido():
    """Un tipo de auth desconocido falla."""
    with pytest.raises(ValueError):
        getApiConfig("oauth")
