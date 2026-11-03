"""Clases del Job 1: extracción desde la API y guardado en GCS.

PROYECTO: Pipeline ETL en GCP - Capa Bronce
NOMBRE: classes.py
ARCHIVOS DESTINO: gs://<BUCKET>/<fuente>/<endpoint>/tipo=<tipo>/
    fecha_datos=<FECHA_DATOS>/ejecucion=<YYYYMMDDTHHMMSSZ>/
    <fuente>_<endpoint>_<inc|snap>_<YYYYMMDD>_p<NNNNN>.json y
    _manifest.json
ARCHIVOS FUENTES: API configurada en parameters.json
OBJETIVO: Extraer los datos crudos de la API y guardarlos sin cambios,
    página por página, con un manifest por endpoint y ejecución.
TIPO: Módulo de clases
OBSERVACION: Cada método hace una sola tarea. Un endpoint que falla no
    detiene a los demás; el job termina con error al final.
SCHEDULER: Airflow 3, diario, FECHA_DATOS = día anterior (D-1)
VERSION: 1.2
DESARROLLADOR: Jorge
PROVEEDOR: Frankfurter (laboratorio)
FECHA: 2026-11-03
DESCRIPCION: Define ClassExtractApi (alias CEA en main.py) y
    ClassLogIngesta (alias CLI), que escribe en la tabla de control.
"""

# seccion Imports
import hashlib
import json
import logging

import requests
from google.cloud import storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.utils import (
    ahoraUtc,
    calcularVentana,
    formatearUtc,
    generarEjecucion,
)

log = logging.getLogger(__name__)

ESTADO_OK = "OK"
ESTADO_FALLIDO = "FALLIDO"
ESTADO_INCOMPLETO = "INCOMPLETO"
ESTADO_OMITIDO = "OMITIDO"
ESTADO_INICIADO = "INICIADO"
TIPO_CORTO = {"incremental": "inc", "snapshot": "snap"}
LARGO_MAXIMO_ERROR = 1000


# seccion Funciones
class ClassLogIngesta:
    """Inserta eventos en la tabla de control ctl_log_ingesta.

    Solo agrega filas, nunca actualiza. Si la inserción falla, deja un
    warning en el log y no detiene el job: la fuente de verdad es el
    manifest del bucket.

    :param tablaLog: tabla completa, proyecto.dataset.tabla
    :type tablaLog: str
    :param clienteBigquery: cliente de BigQuery; si es None se crea uno
    :type clienteBigquery: google.cloud.bigquery.Client or None
    """

    def __init__(self, tablaLog: str, clienteBigquery=None) -> None:
        """Guarda la tabla y crea el cliente si hace falta."""
        if clienteBigquery is None:
            from google.cloud import bigquery

            clienteBigquery = bigquery.Client()
        self.tablaLog = tablaLog
        self.cliente = clienteBigquery

    def registrar(self, fila: dict) -> None:
        """Inserta una fila en la tabla de control.

        :param fila: evento con las columnas de ctl_log_ingesta
        :type fila: dict
        """
        try:
            errores = self.cliente.insert_rows_json(self.tablaLog, [fila])
            if errores:
                log.warning(
                    "No se pudo registrar en %s: %s", self.tablaLog, errores
                )
        except Exception as error:  # noqa: BLE001 - el log no detiene
            log.warning("No se pudo registrar en %s: %s", self.tablaLog, error)


class ClassExtractApi:
    """Extrae los endpoints de la API y guarda el crudo en un bucket.

    :param apiConfig: secreto con apiUrl y las credenciales de la auth
    :type apiConfig: dict
    :param parametros: contenido de config/parameters.json
    :type parametros: dict
    :param nombreBucket: bucket de crudos
    :type nombreBucket: str
    :param contexto: variables de la ejecución (ver utils.getContexto)
    :type contexto: dict
    :param clienteStorage: cliente de GCS; si es None se crea uno
    :type clienteStorage: google.cloud.storage.Client or None
    :param logIngesta: tabla de control; si es None no se registra
    :type logIngesta: ClassLogIngesta or None
    """

    def __init__(
        self,
        apiConfig: dict,
        parametros: dict,
        nombreBucket: str,
        contexto: dict,
        clienteStorage: storage.Client = None,
        logIngesta: ClassLogIngesta = None,
    ) -> None:
        """Inicializa la sesión HTTP y el bucket de destino."""
        self.apiConfig = apiConfig
        self.parametros = parametros
        self.contexto = contexto
        self.logIngesta = logIngesta
        self.urlBase = apiConfig["apiUrl"].rstrip("/")
        self.sesion = self.crearSesion()
        cliente = clienteStorage or storage.Client()
        self.bucket = cliente.bucket(nombreBucket)

    # --- Sesión HTTP ---------------------------------------------------
    def crearSesion(self) -> requests.Session:
        """Crea la sesión HTTP con reintentos y autenticación.

        Los reintentos respetan el header Retry-After de la API.

        :return: sesión lista para llamar a la API
        :rtype: requests.Session
        """
        otros = self.parametros["others"]
        reintentos = Retry(
            total=otros["reintentos"],
            backoff_factor=otros["backoffFactor"],
            status_forcelist=otros["codigosReintento"],
            respect_retry_after_header=True,
        )
        sesion = requests.Session()
        sesion.mount("https://", HTTPAdapter(max_retries=reintentos))
        self.aplicarAuth(sesion)
        return sesion

    def aplicarAuth(self, sesion: requests.Session) -> None:
        """Agrega a la sesión la autenticación de parameters.json.

        Tipos: none, basic (usuario y contraseña), bearer y apiKey.

        :param sesion: sesión HTTP a configurar
        :type sesion: requests.Session
        """
        auth = self.parametros["auth"]
        cfg = self.apiConfig
        if auth["tipo"] == "basic":
            sesion.auth = (cfg["username"], cfg["password"])
        elif auth["tipo"] == "bearer":
            sesion.headers["Authorization"] = f"Bearer {cfg['token']}"
        elif auth["tipo"] == "apiKey":
            sesion.headers[auth["headerApiKey"]] = cfg["apiKey"]

    # --- Parámetros y rutas --------------------------------------------
    def tipoEfectivo(self, nombreEndpoint: str) -> str:
        """Decide si un endpoint se extrae como incremental o snapshot.

        Un endpoint con modo snapshot (por ejemplo, un catálogo) siempre
        se extrae completo. Con TIPO_CARGA=snapshot, todos se extraen
        completos.

        :param nombreEndpoint: clave del endpoint en parameters.json
        :type nombreEndpoint: str
        :return: incremental o snapshot
        :rtype: str
        """
        modo = self.parametros["endpoints"][nombreEndpoint]["modo"]
        if self.contexto["tipoCarga"] == "snapshot" or modo == "snapshot":
            return "snapshot"
        return "incremental"

    def calcularVentana(self, fecha: str, tipo: str) -> tuple:
        """Devuelve (modifiedFrom, modifiedTo) o (None, None) en snapshot.

        :param fecha: fecha de datos (YYYY-MM-DD)
        :type fecha: str
        :param tipo: incremental o snapshot
        :type tipo: str
        :return: inicio y fin de la ventana
        :rtype: tuple
        """
        if tipo == "snapshot":
            return None, None
        formato = self.parametros["ventana"]["formatoFechaHora"]
        return calcularVentana(fecha, formato)

    def armarParametros(
        self, nombreEndpoint: str, fecha: str, tipo: str
    ) -> dict:
        """Arma los query params de un endpoint, sin los de paginación.

        Usa params siempre y paramsIncrementales solo en incremental.
        En cada valor se reemplazan {fecha}, {modifiedFrom} y
        {modifiedTo}.

        :param nombreEndpoint: clave del endpoint en parameters.json
        :type nombreEndpoint: str
        :param fecha: fecha de datos (YYYY-MM-DD)
        :type fecha: str
        :param tipo: incremental o snapshot
        :type tipo: str
        :return: parámetros de la URL
        :rtype: dict
        """
        endpoint = self.parametros["endpoints"][nombreEndpoint]
        lista = list(endpoint.get("params", []))
        if tipo == "incremental":
            lista += endpoint.get("paramsIncrementales", [])
        desde, hasta = self.calcularVentana(fecha, tipo)
        reemplazos = {
            "{fecha}": fecha,
            "{modifiedFrom}": desde or "",
            "{modifiedTo}": hasta or "",
        }
        parametros = {}
        for p in lista:
            valor = str(p["valor"])
            for marcador, texto in reemplazos.items():
                valor = valor.replace(marcador, texto)
            parametros[p["nombre"]] = valor
        return parametros

    def armarPrefijo(
        self, nombreEndpoint: str, tipo: str, fecha: str, ejecucion: str
    ) -> str:
        """Arma la carpeta de una ejecución dentro del bucket.

        :param nombreEndpoint: clave del endpoint en parameters.json
        :type nombreEndpoint: str
        :param tipo: incremental o snapshot
        :type tipo: str
        :param fecha: fecha de datos (YYYY-MM-DD)
        :type fecha: str
        :param ejecucion: identificador de la ejecución
        :type ejecucion: str
        :return: prefijo terminado en /, sin el nombre del bucket
        :rtype: str
        """
        return self.parametros["paths"]["plantillaPrefijo"].format(
            fuente=self.parametros["fuente"],
            endpoint=nombreEndpoint,
            tipo=tipo,
            fecha=fecha,
            ejecucion=ejecucion,
        )

    def armarNombreArchivo(
        self, nombreEndpoint: str, tipo: str, fecha: str, pagina: int
    ) -> str:
        """Arma el nombre del JSON de una página.

        :param nombreEndpoint: clave del endpoint en parameters.json
        :type nombreEndpoint: str
        :param tipo: incremental o snapshot
        :type tipo: str
        :param fecha: fecha de datos (YYYY-MM-DD)
        :type fecha: str
        :param pagina: número de página, desde 1
        :type pagina: int
        :return: por ejemplo frankfurter_rates_inc_20261102_p00001.json
        :rtype: str
        """
        return self.parametros["paths"]["plantillaArchivo"].format(
            fuente=self.parametros["fuente"],
            endpoint=nombreEndpoint,
            tipoCorto=TIPO_CORTO[tipo],
            fechaCompacta=fecha.replace("-", ""),
            pagina=pagina,
            extension=self.parametros["filesExt"]["extension"],
        )

    # --- API y bucket --------------------------------------------------
    def llamarApi(self, ruta: str, parametros: dict):
        """Llama a la API y devuelve su respuesta.

        :param ruta: ruta del endpoint, por ejemplo /rates
        :type ruta: str
        :param parametros: query params
        :type parametros: dict
        :return: respuesta JSON de la API
        :rtype: list or dict
        :raises requests.HTTPError: si la API responde con error
        """
        respuesta = self.sesion.get(
            self.urlBase + ruta,
            params=parametros,
            timeout=self.parametros["others"]["timeoutSegundos"],
        )
        respuesta.raise_for_status()
        return respuesta.json()

    def guardarCrudo(self, datos, rutaDestino: str) -> dict:
        """Guarda los datos sin cambios como JSON en el bucket.

        :param datos: respuesta de la API o manifest
        :type datos: list or dict
        :param rutaDestino: ruta del objeto dentro del bucket
        :type rutaDestino: str
        :return: tamaño en bytes y md5 (hex) del contenido subido
        :rtype: dict
        """
        contenido = json.dumps(datos, ensure_ascii=False).encode("utf-8")
        self.bucket.blob(rutaDestino).upload_from_string(
            contenido,
            content_type=self.parametros["filesExt"]["contentType"],
        )
        return {
            "bytes": len(contenido),
            "md5": hashlib.md5(contenido).hexdigest(),
        }

    def buscarEjecucionOk(self, nombreEndpoint: str, tipo: str, fecha: str):
        """Busca una ejecución con manifest OK para el mismo día y tipo.

        :param nombreEndpoint: clave del endpoint en parameters.json
        :type nombreEndpoint: str
        :param tipo: incremental o snapshot
        :type tipo: str
        :param fecha: fecha de datos (YYYY-MM-DD)
        :type fecha: str
        :return: ruta de la carpeta OK más reciente, o None
        :rtype: str or None
        """
        prefijoDia = self.armarPrefijo(nombreEndpoint, tipo, fecha, "")
        prefijoDia = prefijoDia[: prefijoDia.rindex("ejecucion=")]
        nombreManifest = "/" + self.parametros["paths"]["nombreManifest"]
        manifests = sorted(
            (
                b
                for b in self.bucket.list_blobs(prefix=prefijoDia)
                if b.name.endswith(nombreManifest)
            ),
            key=lambda b: b.name,
            reverse=True,
        )
        for blob in manifests:
            manifest = json.loads(blob.download_as_text())
            if manifest.get("estado") == ESTADO_OK:
                return blob.name[: -len(nombreManifest) + 1]
        return None

    # --- Paginación y manifest -----------------------------------------
    def contarItems(self, respuesta, paginado: bool) -> int:
        """Cuenta los registros de una respuesta.

        :param respuesta: respuesta JSON de la API
        :type respuesta: list or dict
        :param paginado: si el endpoint usa paginación
        :type paginado: bool
        :return: cantidad de registros
        :rtype: int
        """
        campoItems = self.parametros["paginacion"]["campoItems"]
        if isinstance(respuesta, dict) and isinstance(
            respuesta.get(campoItems), list
        ):
            return len(respuesta[campoItems])
        if paginado:
            raise ValueError(f"La respuesta no tiene la lista {campoItems!r}")
        return len(respuesta)

    def iniciarManifest(
        self,
        nombreEndpoint: str,
        tipo: str,
        fecha: str,
        ejecucion: str,
        inicio,
    ) -> dict:
        """Crea el manifest de una ejecución, todavía sin páginas.

        No guarda credenciales ni headers de autenticación.

        :param nombreEndpoint: clave del endpoint en parameters.json
        :type nombreEndpoint: str
        :param tipo: incremental o snapshot
        :type tipo: str
        :param fecha: fecha de datos (YYYY-MM-DD)
        :type fecha: str
        :param ejecucion: identificador de la ejecución
        :type ejecucion: str
        :param inicio: momento de inicio en UTC
        :type inicio: datetime
        :return: manifest inicial
        :rtype: dict
        """
        desde, hasta = self.calcularVentana(fecha, tipo)
        endpoint = self.parametros["endpoints"][nombreEndpoint]
        ctx = self.contexto
        return {
            "fuente": self.parametros["fuente"],
            "endpoint": nombreEndpoint,
            "tipo": tipo,
            "fecha_datos": fecha,
            "ventana": {"modifiedFrom": desde, "modifiedTo": hasta},
            "ejecucion": ejecucion,
            "ruta_gcs": "gs://{}/{}".format(
                self.bucket.name,
                self.armarPrefijo(nombreEndpoint, tipo, fecha, ejecucion),
            ),
            "estado": ESTADO_INICIADO,
            "inicio_utc": formatearUtc(inicio),
            "fin_utc": None,
            "duracion_seg": None,
            "airflow": {
                "dag_id": ctx["dagId"],
                "run_id": ctx["airflowRunId"],
                "intento": ctx["intento"],
            },
            "cloud_run_execution": ctx["cloudRunExecution"],
            "request": {
                "ruta": endpoint["ruta"],
                "params": self.armarParametros(nombreEndpoint, fecha, tipo),
            },
            "paginado": endpoint.get("paginado", False),
            "page_size": None,
            "total_count": None,
            "total_pages": None,
            "registros_descargados": 0,
            "archivos": [],
            "observaciones": [],
            "error": None,
        }

    def descargarPaginas(self, manifest: dict, prefijo: str) -> None:
        """Descarga todas las páginas y guarda cada una apenas llega.

        Sin paginación hace una sola llamada. Con paginación lee
        totalCount y totalPages de la primera página y sigue hasta
        totalPages. Completa el manifest a medida que avanza.

        :param manifest: manifest de la ejecución (se modifica)
        :type manifest: dict
        :param prefijo: carpeta de la ejecución en el bucket
        :type prefijo: str
        :raises RuntimeError: si se supera maxPaginas
        """
        cfg = self.parametros["paginacion"]
        nombreEndpoint = manifest["endpoint"]
        ruta = manifest["request"]["ruta"]
        paginado = manifest["paginado"]
        parametros = dict(manifest["request"]["params"])
        if paginado:
            manifest["page_size"] = cfg["tamanoPagina"]
            parametros[cfg["paramTamano"]] = cfg["tamanoPagina"]
        pagina = 1
        while True:
            if paginado:
                parametros[cfg["paramPagina"]] = (
                    cfg["paginaInicial"] + pagina - 1
                )
            respuesta = self.llamarApi(ruta, dict(parametros))
            nombre = self.armarNombreArchivo(
                nombreEndpoint,
                manifest["tipo"],
                manifest["fecha_datos"],
                pagina,
            )
            datosArchivo = self.guardarCrudo(respuesta, prefijo + nombre)
            registros = self.contarItems(respuesta, paginado)
            manifest["archivos"].append(
                {
                    "nombre": nombre,
                    "pagina": pagina,
                    "registros": registros,
                    **datosArchivo,
                }
            )
            manifest["registros_descargados"] += registros
            if not paginado:
                return
            self.leerTotales(manifest, respuesta, pagina)
            if pagina >= (manifest["total_pages"] or 0):
                return
            if pagina >= cfg["maxPaginas"]:
                raise RuntimeError(
                    f"Se superó maxPaginas ({cfg['maxPaginas']})"
                )
            pagina += 1

    def leerTotales(self, manifest: dict, respuesta, pagina: int) -> None:
        """Guarda totalCount y totalPages de la primera página.

        En las siguientes, anota si cambiaron: significa que los datos
        se modificaron mientras se paginaba.

        :param manifest: manifest de la ejecución (se modifica)
        :type manifest: dict
        :param respuesta: respuesta JSON de la página
        :type respuesta: dict
        :param pagina: número de página, desde 1
        :type pagina: int
        """
        cfg = self.parametros["paginacion"]
        totalCount = respuesta.get(cfg["campoTotalCount"])
        totalPages = respuesta.get(cfg["campoTotalPages"])
        if pagina == 1:
            manifest["total_count"] = totalCount
            manifest["total_pages"] = totalPages
        elif (totalCount, totalPages) != (
            manifest["total_count"],
            manifest["total_pages"],
        ):
            manifest["observaciones"].append(
                f"Página {pagina}: totalCount={totalCount}, "
                f"totalPages={totalPages} distintos a la página 1"
            )

    def validarCompletitud(self, manifest: dict) -> list:
        """Compara lo descargado con lo que informó la API.

        :param manifest: manifest con las páginas descargadas
        :type manifest: dict
        :return: lista de problemas; vacía si está completo
        :rtype: list
        """
        if not manifest["paginado"]:
            return []
        problemas = list(manifest["observaciones"])
        if manifest["total_pages"] is None or manifest["total_count"] is None:
            problemas.append("La API no devolvió totalCount o totalPages")
            return problemas
        paginasEsperadas = max(manifest["total_pages"], 1)
        if len(manifest["archivos"]) != paginasEsperadas:
            problemas.append(
                f"Páginas: {len(manifest['archivos'])} de {paginasEsperadas}"
            )
        if manifest["registros_descargados"] != manifest["total_count"]:
            problemas.append(
                f"Registros: {manifest['registros_descargados']} de "
                f"{manifest['total_count']}"
            )
        return problemas

    # --- Tabla de control ----------------------------------------------
    def armarFilaLog(self, manifest: dict, estado: str) -> dict:
        """Convierte el manifest en una fila de ctl_log_ingesta.

        :param manifest: manifest de la ejecución
        :type manifest: dict
        :param estado: estado del evento
        :type estado: str
        :return: fila con las columnas de la tabla de control
        :rtype: dict
        """
        return {
            "ts_evento": formatearUtc(ahoraUtc()),
            "etapa": "EXTRACCION",
            "estado": estado,
            "fuente": manifest["fuente"],
            "endpoint": manifest["endpoint"],
            "tipo": manifest["tipo"],
            "fecha_datos": manifest["fecha_datos"],
            "ventana_desde": manifest["ventana"]["modifiedFrom"],
            "ventana_hasta": manifest["ventana"]["modifiedTo"],
            "ejecucion": manifest["ejecucion"],
            "dag_id": manifest["airflow"]["dag_id"],
            "airflow_run_id": manifest["airflow"]["run_id"],
            "intento": manifest["airflow"]["intento"],
            "cloud_run_execution": manifest["cloud_run_execution"],
            "total_count": manifest["total_count"],
            "total_pages": manifest["total_pages"],
            "registros": manifest["registros_descargados"],
            "ruta_gcs": manifest["ruta_gcs"],
            "error": manifest["error"],
            "duracion_seg": manifest["duracion_seg"],
        }

    def registrarEvento(self, manifest: dict, estado: str) -> None:
        """Registra un evento en la tabla de control, si está configurada.

        :param manifest: manifest de la ejecución
        :type manifest: dict
        :param estado: estado del evento
        :type estado: str
        """
        if self.logIngesta is not None:
            self.logIngesta.registrar(self.armarFilaLog(manifest, estado))

    # --- Orquestación --------------------------------------------------
    def ejecutarEndpoint(self, nombreEndpoint: str, fecha: str) -> str:
        """Extrae un endpoint y escribe su manifest, aunque falle.

        Si ya existe una ejecución OK para el mismo día y tipo, y no se
        pidió FORZAR, lo omite: así un reintento de Airflow solo repite
        los endpoints que fallaron.

        :param nombreEndpoint: clave del endpoint en parameters.json
        :type nombreEndpoint: str
        :param fecha: fecha de datos (YYYY-MM-DD)
        :type fecha: str
        :return: estado final (OK, FALLIDO, INCOMPLETO u OMITIDO)
        :rtype: str
        """
        tipo = self.tipoEfectivo(nombreEndpoint)
        if not self.contexto["forzar"]:
            rutaOk = self.buscarEjecucionOk(nombreEndpoint, tipo, fecha)
            if rutaOk:
                log.info(
                    "%s: ya existe gs://%s/%s, se omite",
                    nombreEndpoint,
                    self.bucket.name,
                    rutaOk,
                )
                ejecucionOk = rutaOk.rstrip("/").split("ejecucion=")[-1]
                manifest = self.iniciarManifest(
                    nombreEndpoint, tipo, fecha, ejecucionOk, ahoraUtc()
                )
                self.registrarEvento(manifest, ESTADO_OMITIDO)
                return ESTADO_OMITIDO

        inicio = ahoraUtc()
        ejecucion = generarEjecucion(inicio)
        prefijo = self.armarPrefijo(nombreEndpoint, tipo, fecha, ejecucion)
        manifest = self.iniciarManifest(
            nombreEndpoint, tipo, fecha, ejecucion, inicio
        )
        self.registrarEvento(manifest, ESTADO_INICIADO)
        try:
            self.descargarPaginas(manifest, prefijo)
            problemas = self.validarCompletitud(manifest)
            if problemas:
                manifest["estado"] = ESTADO_INCOMPLETO
                manifest["error"] = "; ".join(problemas)
            else:
                manifest["estado"] = ESTADO_OK
        except Exception as error:  # noqa: BLE001 - se registra y sigue
            manifest["estado"] = ESTADO_FALLIDO
            manifest["error"] = f"{type(error).__name__}: {error}"[
                :LARGO_MAXIMO_ERROR
            ]
            log.exception("%s: falló la extracción", nombreEndpoint)
        finally:
            fin = ahoraUtc()
            manifest["fin_utc"] = formatearUtc(fin)
            manifest["duracion_seg"] = round((fin - inicio).total_seconds(), 3)
            nombreManifest = self.parametros["paths"]["nombreManifest"]
            self.guardarCrudo(manifest, prefijo + nombreManifest)
            self.registrarEvento(manifest, manifest["estado"])
        log.info(
            "%s (%s): %s, %d registros, %d páginas -> %s",
            nombreEndpoint,
            tipo,
            manifest["estado"],
            manifest["registros_descargados"],
            len(manifest["archivos"]),
            manifest["ruta_gcs"],
        )
        return manifest["estado"]

    def listarEndpoints(self) -> list:
        """Devuelve los endpoints a procesar: ENDPOINTS o todos.

        :return: nombres de endpoints
        :rtype: list
        :raises ValueError: si ENDPOINTS trae un nombre que no existe
        """
        todos = list(self.parametros["endpoints"])
        pedidos = self.contexto["endpoints"]
        if not pedidos:
            return todos
        desconocidos = sorted(set(pedidos) - set(todos))
        if desconocidos:
            raise ValueError(
                f"ENDPOINTS no existen en parameters.json: {desconocidos}"
            )
        return pedidos

    def ejecutar(self, fecha: str) -> dict:
        """Extrae cada endpoint y falla al final si alguno no quedó OK.

        :param fecha: fecha de datos (YYYY-MM-DD)
        :type fecha: str
        :return: estado por endpoint
        :rtype: dict
        :raises RuntimeError: si algún endpoint quedó FALLIDO o
            INCOMPLETO, para que Airflow reintente
        """
        resultados = {
            nombre: self.ejecutarEndpoint(nombre, fecha)
            for nombre in self.listarEndpoints()
        }
        log.info("Resumen %s: %s", fecha, resultados)
        conProblemas = [
            nombre
            for nombre, estado in resultados.items()
            if estado in (ESTADO_FALLIDO, ESTADO_INCOMPLETO)
        ]
        if conProblemas:
            raise RuntimeError(
                f"Endpoints sin completar para {fecha}: {conProblemas}"
            )
        return resultados
