"""Pruebas unitarias de utils/utils.py."""

# seccion Imports
import json
import logging
import re

import pytest

from utils.utils import (
    configurarLogging,
    getConfig,
    getContextoEjecucion,
    getFechaDatos,
    validarFecha,
)


# seccion Funciones
@pytest.fixture(autouse=True)
def fechaDatos(monkeypatch):
    """FECHA_DATOS válida para getContextoEjecucion."""
    monkeypatch.setenv("FECHA_DATOS", "2026-11-02")


def testGetConfigTieneSecciones():
    """parameters.json trae las secciones que usa el job."""
    parametros = getConfig("parameters.json")
    for seccion in (
        "fuente",
        "auth",
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


def testContextoPorDefecto(monkeypatch):
    """Sin variables: todos los endpoints, sin forzar, intento 1."""
    for v in (
        "ENDPOINTS",
        "FORZAR",
        "INTENTO",
        "AIRFLOW_TASK_ID",
    ):
        monkeypatch.delenv(v, raising=False)
    ctx = getContextoEjecucion()
    assert ctx["taskId"] == "manual"
    assert ctx["endpoints"] == []
    assert ctx["forzar"] is False
    assert ctx["intento"] == 1


def testContextoDesdeAirflow(monkeypatch):
    """Lee las variables que envía Airflow."""
    monkeypatch.setenv("ENDPOINTS", "rates, currencies")
    monkeypatch.setenv("FORZAR", "true")
    monkeypatch.setenv("INTENTO", "3")
    monkeypatch.setenv("AIRFLOW_TASK_ID", "extraer_api_a_bucket")
    ctx = getContextoEjecucion()
    assert ctx["fechaDatos"] == "2026-11-02"
    assert ctx["taskId"] == "extraer_api_a_bucket"
    assert ctx["endpoints"] == ["rates", "currencies"]
    assert ctx["forzar"] is True
    assert ctx["intento"] == 3


@pytest.mark.parametrize("valor", ["1", "si", "yes", "false", ""])
def testForzarSoloConTrue(monkeypatch, valor):
    """FORZAR solo se activa con true; cualquier otro valor es false."""
    monkeypatch.setenv("FORZAR", valor)
    assert getContextoEjecucion()["forzar"] is False


def testLogJsonEnCloudRun(monkeypatch, capsys):
    """En Cloud Run cada log es JSON de StructuredLogHandler."""
    monkeypatch.setenv("CLOUD_RUN_JOB", "job-extract")
    configurarLogging()
    log = logging.getLogger("prueba")
    log.warning("sin tabla", extra={"json_fields": {"endpoint": "rates"}})
    try:
        raise ValueError("boom")
    except ValueError:
        log.exception("falló")
    entradas = [
        json.loads(linea)
        for linea in capsys.readouterr().err.strip().splitlines()
    ]
    aviso, error = [e for e in entradas if "message" in e][-2:]
    assert aviso["severity"] == "WARNING"
    assert aviso["message"] == "sin tabla"
    assert aviso["endpoint"] == "rates"
    assert error["severity"] == "ERROR"
    assert "ValueError: boom" in error["message"]


def testLogTextoEnLocal(monkeypatch, capsys):
    """Sin CLOUD_RUN_JOB el log es texto legible."""
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    configurarLogging()
    logging.getLogger("prueba").info("hola")
    salida = capsys.readouterr().err.strip()
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \[INFO   \] prueba: hola", salida
    )
