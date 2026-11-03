-- Tabla de control de la ingesta: una fila por evento, solo inserciones (nunca UPDATE).
-- El job 1 escribe etapa = EXTRACCION; el job 2 escribirá etapa = CARGA.
-- Laboratorio: dataset de bronce ext_comex en jclab-dev-brz-tmp.
CREATE TABLE IF NOT EXISTS `jclab-dev-brz-tmp.ext_comex.ctl_log_ingesta` (
  ts_evento TIMESTAMP NOT NULL,
  etapa STRING NOT NULL,            -- EXTRACCION / CARGA
  estado STRING NOT NULL,           -- INICIADO / OK / FALLIDO / INCOMPLETO / OMITIDO
  fuente STRING NOT NULL,
  endpoint STRING NOT NULL,
  tipo STRING,                      -- incremental / snapshot
  fecha_datos DATE,
  ventana_desde TIMESTAMP,          -- modifiedFrom (vacío en snapshot)
  ventana_hasta TIMESTAMP,          -- modifiedTo (vacío en snapshot)
  ejecucion STRING,                 -- carpeta ejecucion=YYYYMMDDTHHMMSSZ
  dag_id STRING,
  airflow_run_id STRING,
  intento INT64,
  cloud_run_execution STRING,
  total_count INT64,
  total_pages INT64,
  registros INT64,
  ruta_gcs STRING,
  error STRING,
  duracion_seg FLOAT64
)
PARTITION BY DATE(ts_evento)
CLUSTER BY fuente, endpoint;

-- Estado actual de cada endpoint y día (última fila por etapa):
-- SELECT * FROM `jclab-dev-brz-tmp.ext_comex.ctl_log_ingesta`
-- WHERE DATE(ts_evento) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
-- QUALIFY ROW_NUMBER() OVER (
--   PARTITION BY fuente, endpoint, tipo, fecha_datos, etapa ORDER BY ts_evento DESC) = 1;
