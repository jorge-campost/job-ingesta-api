-- Tabla de control de la ingesta: una fila por evento, solo inserciones (nunca UPDATE).
-- El job 1 escribe tip_etapa = EXTRACCION; el job 2 escribirá tip_etapa = CARGA.
-- Laboratorio: dataset de bronce ext__frankfurter en jclab-dev-brz-tmp.
-- Nombres según la nomenclatura de bronce: prefijo de tipo (fec, tip, est, nom, cod, num, cnt, val, des).
CREATE TABLE IF NOT EXISTS `jclab-dev-brz-tmp.ext__frankfurter.control_ingesta` (
  fec_evento TIMESTAMP NOT NULL,
  tip_etapa STRING NOT NULL,              -- EXTRACCION / CARGA
  est_evento STRING NOT NULL,             -- INICIADO / OK / FALLIDO / INCOMPLETO / OMITIDO
  nom_fuente STRING NOT NULL,
  nom_endpoint STRING NOT NULL,
  tip_carga STRING,                       -- incremental / snapshot (modo en parameters.json)
  fec_datos DATE,
  val_parametros_dinamicos STRING,        -- JSON con los params que vinieron de {env:...}
  cod_ejecucion STRING,                   -- carpeta ejecucion=YYYYMMDDTHHMMSSZ
  cod_dag STRING,
  cod_tarea STRING,
  cod_ejecucion_airflow STRING,           -- run del DAG: agrupa endpoints e intentos
  num_intento INT64,                      -- try_number de la tarea de Airflow
  cod_ejecucion_cloud_run STRING,         -- una ejecución del job
  ind_forzado BOOL,                       -- true si se ejecutó con FORZAR=true
  cnt_registros_api INT64,                -- totalCount informado por la API
  cnt_paginas_api INT64,                  -- totalPages informado por la API
  cnt_registros_descargados INT64,
  des_ruta_bucket STRING,
  des_error STRING,
  num_duracion_segundos FLOAT64
)
PARTITION BY DATE(fec_evento)
CLUSTER BY nom_endpoint
OPTIONS (
  description = "Control de la ingesta de bronce: una fila por evento de cada endpoint (INICIADO, OK, FALLIDO, INCOMPLETO, OMITIDO). Solo inserciones; la escriben el job de extracción y el job de carga."
);
