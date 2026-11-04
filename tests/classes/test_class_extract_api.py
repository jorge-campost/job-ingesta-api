"""Pruebas de ClassExtractApi, sin llamar a GCP ni a la API."""

# seccion Imports
import json
from unittest.mock import MagicMock

import pytest
import requests

from tests.classes.apoyo import (
    CURRENCIES,
    FECHA,
    RATES,
    crearExtractor,
    paginas,
    parametrosPaginados,
)
from utils.utils import getConfig


# seccion Funciones
@pytest.mark.parametrize(
    "tipo, header, valor",
    [("bearer", "Authorization", "Bearer tk"), ("apiKey", "X-API-Key", "ak")],
)
def testAuthPorHeader(tipo, header, valor):
    """Bearer y apiKey se envían como header."""
    parametros = getConfig()
    parametros["auth"]["tipo"] = tipo
    assert crearExtractor(parametros).sesion.headers[header] == valor


def testAuthBasic():
    """Basic envía usuario y contraseña."""
    parametros = getConfig()
    parametros["auth"]["tipo"] = "basic"
    assert crearExtractor(parametros).sesion.auth == ("usuario", "clave")


def testRutas():
    """Prefijo y nombre de archivo siguen las plantillas."""
    extractor = crearExtractor()
    assert extractor.armarPrefijo(
        "rates", "incremental", FECHA, "20261103T112030Z"
    ) == (
        "frankfurter/rates/tipo=incremental/fecha_datos=2026-11-02/"
        "ejecucion=20261103T112030Z/"
    )
    assert (
        extractor.armarNombreArchivo("rates", "snapshot", FECHA, 3)
        == "frankfurter_rates_snap_20261102_p00003.json"
    )


def testTipoEfectivo():
    """Un catálogo siempre es snapshot; TIPO_CARGA=snapshot gana."""
    extractor = crearExtractor()
    assert extractor.tipoEfectivo("rates") == "incremental"
    assert extractor.tipoEfectivo("currencies") == "snapshot"
    snapshot = crearExtractor(tipoCarga="snapshot")
    assert snapshot.tipoEfectivo("rates") == "snapshot"


def testParametrosIncrementalesYSnapshot():
    """La ventana D-1 solo se envía en incremental."""
    extractor = crearExtractor(parametrosPaginados())
    assert extractor.armarParametros("rates", FECHA, "incremental") == {
        "date": FECHA,
        "modifiedFrom": "2026-11-02T00:00:00",
        "modifiedTo": "2026-11-03T00:00:00",
    }
    assert extractor.armarParametros("rates", FECHA, "snapshot") == {}


def testSinPaginacion():
    """Frankfurter: una llamada, un archivo y manifest OK."""
    extractor = crearExtractor(endpoints=["rates"])
    extractor.llamarApi = MagicMock(return_value=RATES)
    assert extractor.ejecutar(FECHA) == {"rates": "OK"}
    extractor.llamarApi.assert_called_once_with("/rates", {"date": FECHA})
    (manifest,) = extractor.bucket.manifests()
    assert manifest["estado"] == "OK"
    assert manifest["registros_descargados"] == 3
    assert manifest["archivos"][0]["nombre"] == (
        "frankfurter_rates_inc_20261102_p00001.json"
    )
    assert "clave" not in json.dumps(manifest)


def testPaginacionCompleta():
    """Recorre totalPages, guarda cada página y valida totalCount."""
    extractor = crearExtractor(parametrosPaginados())
    respuestas = paginas(RATES[:2], RATES[2:])
    extractor.llamarApi = MagicMock(side_effect=respuestas)
    extractor.ejecutar(FECHA)
    llamadas = [c.args[1]["page"] for c in extractor.llamarApi.call_args_list]
    assert llamadas == [1, 2]
    assert extractor.llamarApi.call_args.args[1]["pageSize"] == 2
    (manifest,) = extractor.bucket.manifests()
    assert manifest["estado"] == "OK"
    assert manifest["total_count"] == 3
    assert [a["registros"] for a in manifest["archivos"]] == [2, 1]


def testDiaSinCambios():
    """totalCount 0 guarda p00001 vacío y queda OK."""
    extractor = crearExtractor(parametrosPaginados())
    extractor.llamarApi = MagicMock(
        return_value={"items": [], "totalCount": 0, "totalPages": 0}
    )
    extractor.ejecutar(FECHA)
    (manifest,) = extractor.bucket.manifests()
    assert manifest["estado"] == "OK"
    assert len(manifest["archivos"]) == 1


def testConteoNoCuadra():
    """Si la suma de items no es totalCount, queda INCOMPLETO y falla."""
    extractor = crearExtractor(parametrosPaginados())
    extractor.llamarApi = MagicMock(
        side_effect=paginas(RATES[:2], RATES[2:], total=4)
    )
    with pytest.raises(RuntimeError):
        extractor.ejecutar(FECHA)
    (manifest,) = extractor.bucket.manifests()
    assert manifest["estado"] == "INCOMPLETO"
    assert "Registros: 3 de 4" in manifest["error"]


def testFallaEnPagina2():
    """Un error HTTP deja manifest FALLIDO con las páginas bajadas."""
    extractor = crearExtractor(parametrosPaginados())
    primera = paginas(RATES[:2], RATES[2:])[0]
    extractor.llamarApi = MagicMock(
        side_effect=[primera, requests.HTTPError("503 Server Error")]
    )
    with pytest.raises(RuntimeError):
        extractor.ejecutar(FECHA)
    (manifest,) = extractor.bucket.manifests()
    assert manifest["estado"] == "FALLIDO"
    assert "503" in manifest["error"]
    assert len(manifest["archivos"]) == 1


def testUnEndpointFallidoNoDetieneALosDemas():
    """currencies se extrae aunque rates falle."""
    extractor = crearExtractor()
    extractor.llamarApi = MagicMock(
        side_effect=[requests.HTTPError("500"), CURRENCIES]
    )
    with pytest.raises(RuntimeError, match="rates"):
        extractor.ejecutar(FECHA)
    estados = {
        m["endpoint"]: m["estado"] for m in extractor.bucket.manifests()
    }
    assert estados == {"rates": "FALLIDO", "currencies": "OK"}


def testReintentoOmiteEndpointsOk():
    """En el intento 2 solo se vuelve a extraer el que falló."""
    extractor = crearExtractor()
    extractor.llamarApi = MagicMock(
        side_effect=[requests.HTTPError("500"), CURRENCIES]
    )
    with pytest.raises(RuntimeError):
        extractor.ejecutar(FECHA)
    extractor.contexto["intento"] = 2
    extractor.llamarApi = MagicMock(return_value=RATES)
    assert extractor.ejecutar(FECHA) == {
        "rates": "OK",
        "currencies": "OMITIDO",
    }
    extractor.llamarApi.assert_called_once()


def testForzarVuelveAExtraer():
    """Con FORZAR no se omite aunque exista una ejecución OK."""
    extractor = crearExtractor(endpoints=["currencies"])
    extractor.llamarApi = MagicMock(return_value=CURRENCIES)
    extractor.ejecutar(FECHA)
    extractor.contexto["forzar"] = True
    assert extractor.ejecutar(FECHA) == {"currencies": "OK"}
    assert extractor.llamarApi.call_count == 2


def testEndpointDesconocido():
    """ENDPOINTS con un nombre que no existe falla antes de extraer."""
    with pytest.raises(ValueError):
        crearExtractor(endpoints=["nada"]).ejecutar(FECHA)


def testRegistraEventosEnLog():
    """Cada endpoint deja INICIADO y su estado final en la tabla."""
    logIngesta = MagicMock()
    extractor = crearExtractor(logIngesta=logIngesta, endpoints=["rates"])
    extractor.llamarApi = MagicMock(return_value=RATES)
    extractor.ejecutar(FECHA)
    filas = [c.args[0] for c in logIngesta.registrar.call_args_list]
    assert [f["estado"] for f in filas] == ["INICIADO", "OK"]
    assert filas[1]["ventana_desde"] == "2026-11-02T00:00:00"
    assert filas[1]["registros"] == 3


def testNivelDeLogSegunEstado(caplog):
    """OK se registra como INFO; FALLIDO como ERROR con sus campos."""
    import logging

    extractor = crearExtractor()
    extractor.llamarApi = MagicMock(
        side_effect=[requests.HTTPError("500"), CURRENCIES]
    )
    with caplog.at_level(logging.INFO), pytest.raises(RuntimeError):
        extractor.ejecutar(FECHA)
    finales = {
        r.json_fields["endpoint"]: r.levelname
        for r in caplog.records
        if "páginas" in r.getMessage()
    }
    assert finales == {"rates": "ERROR", "currencies": "INFO"}
    resumen = [r for r in caplog.records if "Resumen" in r.getMessage()]
    assert resumen[0].levelname == "ERROR"
