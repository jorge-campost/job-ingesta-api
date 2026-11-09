"""Clases del Job 1: extracción desde la API y guardado en GCS.

PROYECTO        : Pipeline ETL en GCP - Capa Bronce
NOMBRE          : classes.py
ARCHIVOS DESTINO: gs://<BUCKET>/<fuente>/<carpeta>/fecha_datos=<FECHA_DATOS>/
                  tipo_carga=<modo>/ejecucion=<YYYYMMDDTHHMMSSZ>/
                  <fuente>_<carpeta>_<inc|snap>_<YYYYMMDD>_p<NNNNN>.json y
                  _manifest.json
ARCHIVOS FUENTES: API configurada en parameters.json
OBJETIVO        : Extraer los datos crudos de la API y guardarlos sin cambios,
                  página por página, con un manifest por endpoint y ejecución.
TIPO            : Módulo de clases
OBSERVACION     : Cada método hace una sola tarea. Un endpoint que falla no
                  detiene a los demás; el job termina con error al final.
SCHEDULER       : Airflow 3, diario, FECHA_DATOS = día anterior (D-1)
VERSION         : 1.5
DESARROLLADOR   : Jorge
PROVEEDOR       : Frankfurter (laboratorio)
FECHA           : 2026-11-06
DESCRIPCION     : Define ClassExtractApi, que extrae y guarda en GCS, y
                  ClassLogIngesta, que escribe en la tabla de control.
"""

# seccion Imports
import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone

import requests
from google.cloud import bigquery, storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.utils import FORMATO_EJECUCION, FORMATO_UTC

log = logging.getLogger(__name__)

ESTADO_OK = "OK"
ESTADO_FALLIDO = "FALLIDO"
ESTADO_INCOMPLETO = "INCOMPLETO"
ESTADO_OMITIDO = "OMITIDO"
ESTADO_INICIADO = "INICIADO"
TIPO_CORTO = {"incremental": "inc", "snapshot": "snap"}
LARGO_MAXIMO_ERROR = 1000
# Marcador de parameters.json que toma el valor de una variable de entorno
PATRON_ENV = re.compile(r"\{env:([A-Za-z_][A-Za-z0-9_]*)\}")
# Claves que debe tener el secreto API_CONFIG según el tipo de auth
CAMPOS_POR_AUTH = {
    "none": {"apiUrl"},
    "basic": {"apiUrl", "username", "password"},
    "bearer": {"apiUrl", "token"},
    "apiKey": {"apiUrl", "apiKey"},
}


# seccion Funciones
class ClassLogIngesta:
    """Inserta eventos en la tabla de control control_ingesta.

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
        self.tablaLog = tablaLog
        self.cliente = clienteBigquery or bigquery.Client()

    def registrar(self, manifest: dict, estado: str) -> None:
        """Inserta en la tabla de control un evento de la ejecución.

        :param manifest: manifest de la ejecución
        :type manifest: dict
        :param estado: estado del evento
        :type estado: str
        """
        fila = {
            "fec_evento": datetime.now(timezone.utc).strftime(FORMATO_UTC),
            "tip_etapa": "EXTRACCION",
            "est_evento": estado,
            "nom_fuente": manifest["fuente"],
            "nom_endpoint": manifest["endpoint"],
            "tip_carga": manifest["tipo"],
            "fec_datos": manifest["fecha_datos"],
            "val_parametros_dinamicos": json.dumps(
                manifest["request"]["params_dinamicos"], ensure_ascii=False
            ),
            "cod_ejecucion": manifest["ejecucion"],
            "cod_dag": manifest["airflow"]["dag_id"],
            "cod_tarea": manifest["airflow"]["task_id"],
            "cod_ejecucion_airflow": manifest["airflow"]["run_id"],
            "num_intento": manifest["airflow"]["intento"],
            "cod_ejecucion_cloud_run": manifest["cloud_run_execution"],
            "cnt_registros_api": manifest["total_count"],
            "cnt_paginas_api": manifest["total_pages"],
            "cnt_registros_descargados": manifest["registros_descargados"],
            "des_ruta_bucket": manifest["ruta_gcs"],
            "des_error": manifest["error"],
            "num_duracion_segundos": manifest["duracion_seg"],
        }
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
    :param parametrosApi: contenido de config/parameters.json
    :type parametrosApi: dict
    :param nombreBucket: bucket de crudos
    :type nombreBucket: str
    :param contextoEjecucion: variables de la ejecución (ver
        utils.getContextoEjecucion)
    :type contextoEjecucion: dict
    :param logIngesta: tabla de control
    :type logIngesta: ClassLogIngesta
    :param clienteStorage: cliente de GCS; si es None se crea uno
    :type clienteStorage: google.cloud.storage.Client or None
    """

    def __init__(
        self,
        apiConfig: dict,
        parametrosApi: dict,
        nombreBucket: str,
        contextoEjecucion: dict,
        logIngesta: ClassLogIngesta,
        clienteStorage: storage.Client = None,
    ) -> None:
        """Inicializa la sesión HTTP y el bucket de destino."""
        self.apiConfig = apiConfig
        self.parametrosApi = parametrosApi
        self.contextoEjecucion = contextoEjecucion
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
        otros = self.parametrosApi["others"]
        reintentos = Retry(
            total=otros["reintentos"],
            backoff_factor=otros["backoffFactor"],
            status_forcelist=otros["codigosReintento"],
            respect_retry_after_header=True,
        )
        sesion = requests.Session()
        sesion.mount("https://", HTTPAdapter(max_retries=reintentos))
        sesion.auth, headers = self.armarAutenticacion()
        sesion.headers.update(headers)
        return sesion

    def armarAutenticacion(self) -> tuple:
        """Arma la autenticación según auth.tipo de parameters.json.

        Tipos: none, basic (usuario y contraseña), bearer y apiKey.
        Antes valida que API_CONFIG traiga las claves de ese tipo.

        :return: (auth para requests o None, headers a agregar)
        :rtype: tuple
        :raises ValueError: si el tipo no existe o faltan claves
        """
        auth = self.parametrosApi["auth"]
        cfg = self.apiConfig
        if auth["tipo"] not in CAMPOS_POR_AUTH:
            raise ValueError(
                f"auth.tipo {auth['tipo']!r} no válido. "
                f"Usa uno de {sorted(CAMPOS_POR_AUTH)}"
            )
        faltantes = CAMPOS_POR_AUTH[auth["tipo"]] - cfg.keys()
        if faltantes:
            raise ValueError(
                f"API_CONFIG no tiene los campos {sorted(faltantes)}"
            )
        if auth["tipo"] == "basic":
            return (cfg["username"], cfg["password"]), {}
        if auth["tipo"] == "bearer":
            return None, {"Authorization": f"Bearer {cfg['token']}"}
        if auth["tipo"] == "apiKey":
            return None, {auth["headerApiKey"]: cfg["apiKey"]}
        return None, {}

    # --- Parámetros y rutas --------------------------------------------
    def armarParametros(self, nombreEndpoint: str) -> tuple:
        """Arma los query params de un endpoint, sin los de paginación.

        Todo lo que está en la lista params se envía. Un valor fijo va
        tal cual; un valor {env:NOMBRE} toma la variable de entorno
        NOMBRE (del .env en local o de Airflow en Cloud Run).

        :param nombreEndpoint: clave del endpoint en parameters.json
        :type nombreEndpoint: str
        :return: (todos los parámetros, los que vinieron de variables)
        :rtype: tuple
        :raises ValueError: si falta una variable de entorno usada
        """
        parametros = {}
        dinamicos = {}
        for p in self.parametrosApi["endpoints"][nombreEndpoint]["params"]:
            valor = str(p["valor"])
            for variable in PATRON_ENV.findall(valor):
                texto = os.environ.get(variable, "")
                if not texto:
                    raise ValueError(
                        f"Falta la variable {variable} para el parámetro "
                        f"{p['nombre']!r} de {nombreEndpoint}"
                    )
                valor = valor.replace("{env:" + variable + "}", texto)
                dinamicos[p["nombre"]] = valor
            parametros[p["nombre"]] = valor
        return parametros, dinamicos

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
        return self.parametrosApi["paths"]["plantillaPrefijo"].format(
            fuente=self.parametrosApi["fuente"],
            carpeta=self.parametrosApi["endpoints"][nombreEndpoint]["carpeta"],
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
        return self.parametrosApi["paths"]["plantillaArchivo"].format(
            fuente=self.parametrosApi["fuente"],
            carpeta=self.parametrosApi["endpoints"][nombreEndpoint]["carpeta"],
            tipoCorto=TIPO_CORTO[tipo],
            fechaCompacta=fecha.replace("-", ""),
            pagina=pagina,
            extension=self.parametrosApi["filesExt"]["extension"],
        )

    # --- API y bucket --------------------------------------------------
    def llamarApi(self, ruta: str, parametros: dict):
        """Llama a la API y devuelve su respuesta.

        Deja en el log la URL completa del GET (URL base, ruta y query
        params) para ver cómo se armó. Las credenciales van en headers
        o en auth, nunca en la URL.

        :param ruta: ruta del endpoint, por ejemplo /rates
        :type ruta: str
        :param parametros: query params
        :type parametros: dict
        :return: respuesta JSON de la API
        :rtype: list or dict
        :raises requests.HTTPError: si la API responde con error
        """
        urlCompleta = (
            requests.Request("GET", self.urlBase + ruta, params=parametros)
            .prepare()
            .url
        )
        log.info("GET %s", urlCompleta)
        respuesta = self.sesion.get(
            urlCompleta,
            timeout=self.parametrosApi["others"]["timeoutSegundos"],
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
            content_type=self.parametrosApi["filesExt"]["contentType"],
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
        nombreManifest = "/" + self.parametrosApi["paths"]["nombreManifest"]
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
        campoItems = self.parametrosApi["paginacion"]["campoItems"]
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
        endpoint = self.parametrosApi["endpoints"][nombreEndpoint]
        params, dinamicos = self.armarParametros(nombreEndpoint)
        ctx = self.contextoEjecucion
        return {
            "fuente": self.parametrosApi["fuente"],
            "endpoint": nombreEndpoint,
            "carpeta": endpoint["carpeta"],
            "tipo": tipo,
            "fecha_datos": fecha,
            "ejecucion": ejecucion,
            "ruta_gcs": "gs://{}/{}".format(
                self.bucket.name,
                self.armarPrefijo(nombreEndpoint, tipo, fecha, ejecucion),
            ),
            "estado": ESTADO_INICIADO,
            "inicio_utc": inicio.strftime(FORMATO_UTC),
            "fin_utc": None,
            "duracion_seg": None,
            "airflow": {
                "dag_id": ctx["dagId"],
                "task_id": ctx["taskId"],
                "run_id": ctx["airflowRunId"],
                "intento": ctx["intento"],
            },
            "cloud_run_execution": ctx["cloudRunExecution"],
            "request": {
                "ruta": endpoint["ruta"],
                "params": params,
                "params_dinamicos": dinamicos,
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

    def descargarPaginas(self, manifest: dict, prefijo: str) -> dict:
        """Descarga todas las páginas y guarda cada una apenas llega.

        Sin paginación hace una sola llamada. Con paginación lee
        totalCount y totalPages de la primera página y sigue hasta
        totalPages. No modifica el manifest: devuelve lo descargado,
        también si falla a mitad, para que queden las páginas guardadas.

        :param manifest: manifest inicial de la ejecución
        :type manifest: dict
        :param prefijo: carpeta de la ejecución en el bucket
        :type prefijo: str
        :return: page_size, total_count, total_pages, archivos,
            registros_descargados, observaciones y error (None si no
            falló)
        :rtype: dict
        """
        cfg = self.parametrosApi["paginacion"]
        nombreEndpoint = manifest["endpoint"]
        ruta = manifest["request"]["ruta"]
        paginado = manifest["paginado"]
        parametros = dict(manifest["request"]["params"])
        resultado = {
            "page_size": cfg["tamanoPagina"] if paginado else None,
            "total_count": None,
            "total_pages": None,
            "archivos": [],
            "registros_descargados": 0,
            "observaciones": [],
            "error": None,
        }
        if paginado:
            parametros[cfg["paramTamano"]] = cfg["tamanoPagina"]
        pagina = 1
        try:
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
                resultado["archivos"].append(
                    {
                        "nombre": nombre,
                        "pagina": pagina,
                        "registros": registros,
                        **datosArchivo,
                    }
                )
                resultado["registros_descargados"] += registros
                if not paginado:
                    break
                totales = self.leerTotales(respuesta)
                if pagina == 1:
                    resultado["total_count"], resultado["total_pages"] = (
                        totales
                    )
                elif totales != (
                    resultado["total_count"],
                    resultado["total_pages"],
                ):
                    resultado["observaciones"].append(
                        f"Página {pagina}: totalCount={totales[0]}, "
                        f"totalPages={totales[1]} distintos a la página 1"
                    )
                if pagina >= (resultado["total_pages"] or 0):
                    break
                if pagina >= cfg["maxPaginas"]:
                    raise RuntimeError(
                        f"Se superó maxPaginas ({cfg['maxPaginas']})"
                    )
                pagina += 1
        except Exception as error:  # noqa: BLE001 - se registra y sigue
            resultado["error"] = f"{type(error).__name__}: {error}"[
                :LARGO_MAXIMO_ERROR
            ]
            log.exception(
                "%s: falló la extracción",
                nombreEndpoint,
                extra={
                    "json_fields": {
                        **self.camposLog(manifest),
                        "estado": ESTADO_FALLIDO,
                    }
                },
            )
        return resultado

    def leerTotales(self, respuesta: dict) -> tuple:
        """Lee totalCount y totalPages de una página.

        :param respuesta: respuesta JSON de la página
        :type respuesta: dict
        :return: (totalCount, totalPages); None si la API no los trae
        :rtype: tuple
        """
        cfg = self.parametrosApi["paginacion"]
        return (
            respuesta.get(cfg["campoTotalCount"]),
            respuesta.get(cfg["campoTotalPages"]),
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
    def camposLog(self, manifest: dict) -> dict:
        """Campos para filtrar los logs en Cloud Logging.

        :param manifest: manifest de la ejecución
        :type manifest: dict
        :return: fuente, endpoint, tipo, fecha_datos, ejecucion y estado
        :rtype: dict
        """
        return {
            clave: manifest[clave]
            for clave in (
                "fuente",
                "endpoint",
                "tipo",
                "fecha_datos",
                "ejecucion",
                "estado",
            )
        }

    def registrarEvento(self, manifest: dict, estado: str) -> None:
        """Registra un evento en la tabla de control.

        :param manifest: manifest de la ejecución
        :type manifest: dict
        :param estado: estado del evento
        :type estado: str
        """
        self.logIngesta.registrar(manifest, estado)

    # --- Orquestación --------------------------------------------------
    def ejecutarEndpoint(self, nombreEndpoint: str, fecha: str) -> str:
        """Extrae un endpoint y escribe su manifest, aunque falle.

        El tipo es el modo del endpoint en parameters.json. Si ya existe
        una ejecución OK para la misma carpeta, día y tipo, y no se
        pidió FORZAR, lo omite: así un reintento de Airflow solo repite
        los endpoints que fallaron.

        :param nombreEndpoint: clave del endpoint en parameters.json
        :type nombreEndpoint: str
        :param fecha: fecha de datos (YYYY-MM-DD)
        :type fecha: str
        :return: estado final (OK, FALLIDO, INCOMPLETO u OMITIDO)
        :rtype: str
        """
        tipo = self.parametrosApi["endpoints"][nombreEndpoint]["modo"]
        if not self.contextoEjecucion["forzar"]:
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
                    nombreEndpoint,
                    tipo,
                    fecha,
                    ejecucionOk,
                    datetime.now(timezone.utc),
                )
                self.registrarEvento(manifest, ESTADO_OMITIDO)
                return ESTADO_OMITIDO

        inicio = datetime.now(timezone.utc)
        ejecucion = inicio.strftime(FORMATO_EJECUCION)
        prefijo = self.armarPrefijo(nombreEndpoint, tipo, fecha, ejecucion)
        manifest = self.iniciarManifest(
            nombreEndpoint, tipo, fecha, ejecucion, inicio
        )
        self.registrarEvento(manifest, ESTADO_INICIADO)
        manifest = {**manifest, **self.descargarPaginas(manifest, prefijo)}
        if manifest["error"]:
            estado, error = ESTADO_FALLIDO, manifest["error"]
        else:
            problemas = self.validarCompletitud(manifest)
            estado = ESTADO_INCOMPLETO if problemas else ESTADO_OK
            error = "; ".join(problemas) or None
        fin = datetime.now(timezone.utc)
        manifest = {
            **manifest,
            "estado": estado,
            "error": error,
            "fin_utc": fin.strftime(FORMATO_UTC),
            "duracion_seg": round((fin - inicio).total_seconds(), 3),
        }
        nombreManifest = self.parametrosApi["paths"]["nombreManifest"]
        self.guardarCrudo(manifest, prefijo + nombreManifest)
        self.registrarEvento(manifest, estado)
        nivel = (
            logging.INFO if manifest["estado"] == ESTADO_OK else logging.ERROR
        )
        log.log(
            nivel,
            "%s (%s): %s, %d registros, %d páginas -> %s",
            nombreEndpoint,
            tipo,
            manifest["estado"],
            manifest["registros_descargados"],
            len(manifest["archivos"]),
            manifest["ruta_gcs"],
            extra={"json_fields": self.camposLog(manifest)},
        )
        return manifest["estado"]

    def listarEndpoints(self) -> list:
        """Devuelve los endpoints a procesar: ENDPOINTS o todos.

        :return: nombres de endpoints
        :rtype: list
        :raises ValueError: si ENDPOINTS trae un nombre que no existe
        """
        todos = list(self.parametrosApi["endpoints"])
        pedidos = self.contextoEjecucion["endpoints"]
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
        :raises ValueError: si falta una variable {env:...}, antes de
            extraer cualquier endpoint
        :raises RuntimeError: si algún endpoint quedó FALLIDO o
            INCOMPLETO, para que Airflow reintente
        """
        nombres = self.listarEndpoints()
        # Error de configuración: falla antes de bajar nada
        for nombre in nombres:
            self.armarParametros(nombre)
        resultados = {
            nombre: self.ejecutarEndpoint(nombre, fecha) for nombre in nombres
        }
        conProblemas = [
            nombre
            for nombre, estado in resultados.items()
            if estado in (ESTADO_FALLIDO, ESTADO_INCOMPLETO)
        ]
        log.log(
            logging.ERROR if conProblemas else logging.INFO,
            "Resumen %s: %s",
            fecha,
            resultados,
            extra={
                "json_fields": {"fecha_datos": fecha, "resultados": resultados}
            },
        )
        if conProblemas:
            raise RuntimeError(
                f"Endpoints sin completar para {fecha}: {conProblemas}"
            )
        return resultados
