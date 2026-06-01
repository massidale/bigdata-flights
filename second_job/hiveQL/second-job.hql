-- esercitazione_3_2_hive_corretto.hql

SET hive.execution.engine=mr; -- o 'tez' per prestazioni migliori, se disponibile
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 1. Definizione della tabella esterna (mantenuta dalla tua versione)
DROP TABLE IF EXISTS flights;
CREATE EXTERNAL TABLE flights (
  origin STRING,
  month INT,
  dep_delay DOUBLE,
  arr_delay DOUBLE,
  cancelled INT,
  cancellation_code STRING,
  carrier_delay DOUBLE,
  weather_delay DOUBLE,
  nas_delay DOUBLE,
  security_delay DOUBLE,
  late_aircraft_delay DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/root/input'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 2. Query Unica Completa tramite l'uso di CTE (Common Table Expressions)
WITH stats_ritardi AS (
  -- STAGE 1 & 2: Classificazione fasce e calcolo metriche per aeroporto e mese
  SELECT
    origin,
    month,
    SUM(CASE WHEN dep_delay < 15 THEN 1 ELSE 0 END) AS basso_count,
    AVG(CASE WHEN dep_delay < 15 THEN dep_delay ELSE NULL END) AS basso_dep_delay_avg,
    AVG(CASE WHEN dep_delay < 15 THEN arr_delay ELSE NULL END) AS basso_arr_delay_avg,

    SUM(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN 1 ELSE 0 END) AS medio_count,
    AVG(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN dep_delay ELSE NULL END) AS medio_dep_delay_avg,
    AVG(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN arr_delay ELSE NULL END) AS medio_arr_delay_avg,

    SUM(CASE WHEN dep_delay > 60 THEN 1 ELSE 0 END) AS alto_count,
    AVG(CASE WHEN dep_delay > 60 THEN dep_delay ELSE NULL END) AS alto_dep_delay_avg,
    AVG(CASE WHEN dep_delay > 60 THEN arr_delay ELSE NULL END) AS alto_arr_delay_avg
  FROM flights
  WHERE dep_delay IS NOT NULL AND cancelled = 0
  GROUP BY origin, month
),

cause_unpivoted AS (
  -- STAGE 3a: Unpivot ottimizzato tramite LATERAL VIEW e MAP (Evita le 5 UNION ALL)
  SELECT
    origin,
    month,
    cause
  FROM flights
  LATERAL VIEW explode(map(
    'carrier_delay', CASE WHEN carrier_delay > 0 THEN 1 ELSE 0 END,
    'weather_delay', CASE WHEN weather_delay > 0 THEN 1 ELSE 0 END,
    'nas_delay', CASE WHEN nas_delay > 0 THEN 1 ELSE 0 END,
    'security_delay', CASE WHEN security_delay > 0 THEN 1 ELSE 0 END,
    'late_aircraft_delay', CASE WHEN late_aircraft_delay > 0 THEN 1 ELSE 0 END,
    CONCAT('canc_', COALESCE(cancellation_code, 'NA')), CASE WHEN cancelled = 1 AND cancellation_code IS NOT NULL THEN 1 ELSE 0 END
  )) cause_map AS cause, cnt
  WHERE cnt > 0 AND cause IS NOT NULL
),

cause_conteggiate AS (
  -- STAGE 3b: Conteggio frequenza cause per aeroporto e mese
  SELECT
    origin,
    month,
    cause,
    COUNT(*) AS freq
  FROM cause_unpivoted
  GROUP BY origin, month, cause
),

cause_classificate AS (
  -- STAGE 3c: Assegnazione ranking locale
  SELECT
    origin,
    month,
    cause,
    ROW_NUMBER() OVER (PARTITION BY origin, month ORDER BY freq DESC) AS rnk
  FROM cause_conteggiate
),

top_3_cause AS (
  -- STAGE 3d: Estrazione Top 3 per gruppo su singola riga
  SELECT
    origin,
    month,
    MAX(CASE WHEN rnk = 1 THEN cause ELSE NULL END) AS top_causa_1,
    MAX(CASE WHEN rnk = 2 THEN cause ELSE NULL END) AS top_causa_2,
    MAX(CASE WHEN rnk = 3 THEN cause ELSE NULL END) AS top_causa_3
  FROM cause_classificate
  WHERE rnk <= 3
  GROUP BY origin, month
)

-- STAGE 4: Generazione report finale
-- Puoi aggiungere "INSERT OVERWRITE DIRECTORY 'hdfs://...' ROW FORMAT..." prima di questa SELECT per salvare i dati
SELECT
  r.origin,
  r.month,
  r.basso_count, r.basso_dep_delay_avg, r.basso_arr_delay_avg,
  r.medio_count, r.medio_dep_delay_avg, r.medio_arr_delay_avg,
  r.alto_count, r.alto_dep_delay_avg, r.alto_arr_delay_avg,
  COALESCE(c.top_causa_1, 'Nessuna') AS top_causa_1,
  COALESCE(c.top_causa_2, 'Nessuna') AS top_causa_2,
  COALESCE(c.top_causa_3, 'Nessuna') AS top_causa_3
FROM stats_ritardi r
LEFT JOIN top_3_cause c ON r.origin = c.origin AND r.month = c.month
ORDER BY r.origin, r.month;