-- Consultas de ejemplo sobre la tabla de control control_ingesta.
-- fec_evento está en UTC y la tabla se particiona por DATE(fec_evento):
-- filtrar por ese campo reduce los bytes leídos.

-- 1. Eventos de un día específico, en orden cronológico
SELECT fec_evento, tip_etapa, nom_endpoint, tip_carga, fec_datos, est_evento,
       num_intento, cnt_registros_descargados, num_duracion_segundos, des_error
FROM `jclab-dev-brz-tmp.ext__frankfurter.control_ingesta`
WHERE DATE(fec_evento) = '2026-11-03'
ORDER BY fec_evento;

-- 2. Eventos de un endpoint específico en un día específico
SELECT fec_evento, tip_etapa, est_evento, fec_datos, cod_ejecucion,
       num_intento, cnt_registros_api, cnt_registros_descargados, des_ruta_bucket, des_error
FROM `jclab-dev-brz-tmp.ext__frankfurter.control_ingesta`
WHERE DATE(fec_evento) = '2026-11-03'
  AND nom_endpoint = 'rates'
ORDER BY fec_evento;

-- 3. Estado actual de cada endpoint y fecha de datos (última fila por etapa), últimos 7 días
SELECT nom_fuente, nom_endpoint, tip_carga, fec_datos, tip_etapa, est_evento, fec_evento, des_error
FROM `jclab-dev-brz-tmp.ext__frankfurter.control_ingesta`
WHERE DATE(fec_evento) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY nom_fuente, nom_endpoint, tip_carga, fec_datos, tip_etapa
  ORDER BY fec_evento DESC) = 1
ORDER BY fec_datos DESC, nom_endpoint;

-- 4. Todos los endpoints de un run de Airflow, por intento
SELECT cod_ejecucion_airflow, cod_tarea, num_intento, nom_endpoint, est_evento,
       cnt_registros_descargados, des_error
FROM `jclab-dev-brz-tmp.ext__frankfurter.control_ingesta`
WHERE DATE(fec_evento) >= '2026-11-03'
  AND cod_ejecucion_airflow = 'scheduled__2026-11-03T06:00:00+00:00'
  AND est_evento != 'INICIADO'
ORDER BY num_intento, nom_endpoint;
