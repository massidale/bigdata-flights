-- Primo job in HiveQL: statistiche voli per compagnia.
-- Stage 1: aggregazione per (op_unique_carrier, origin)
-- Stage 2: annidamento delle tratte per compagnia in un array<struct>
-- Stage 3: dump dell'output finale come JSON-line su HDFS (formato uniforme
--          con gli altri 3 job per il confronto cross-tecnologia).
--
-- Parametri attesi via sed da run.sh:
--   ${hivevar:input_path}  : LOCATION della tabella esterna (es. hdfs://.../flight/bench_100/)
--   ${hivevar:output_path} : dir HDFS dove dumpare l'output finale come JSON-line

CREATE DATABASE IF NOT EXISTS flight;
USE flight;

-- Schema 12 colonne, output di prep/prepare.py (header-less).
DROP TABLE IF EXISTS flight_raw;
CREATE EXTERNAL TABLE flight_raw (
    op_unique_carrier     STRING,
    origin                STRING,
    month                 INT,
    dep_delay             DOUBLE,
    arr_delay             DOUBLE,
    cancelled             DOUBLE,
    cancellation_code     STRING,
    carrier_delay         DOUBLE,
    weather_delay         DOUBLE,
    nas_delay             DOUBLE,
    security_delay        DOUBLE,
    late_aircraft_delay   DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\"",
    "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '${hivevar:input_path}';

-- Stage 1: stats per (compagnia, aeroporto di origine).
-- Nota: OpenCSVSerde tratta TUTTE le colonne come STRING ignorando i tipi
-- dichiarati nello schema. Senza CAST esplicito, MIN/MAX su arr_delay
-- farebbero confronti lessicografici (sbagliati).
DROP TABLE IF EXISTS stats_per_carrier_origin;
CREATE TABLE stats_per_carrier_origin AS
SELECT
    op_unique_carrier,
    origin,
    COUNT(*)                                                AS numero_voli,
    MIN(CAST(arr_delay AS DOUBLE))                          AS ritardo_minimo_arrivo,
    MAX(CAST(arr_delay AS DOUBLE))                          AS ritardo_massimo_arrivo,
    AVG(CAST(arr_delay AS DOUBLE))                          AS ritardo_medio_arrivo,
    AVG(CAST(cancelled AS DOUBLE))                          AS tasso_cancellazioni,
    sort_array(collect_set(CAST(month AS INT)))             AS mesi_operativi
FROM flight_raw
GROUP BY op_unique_carrier, origin;

-- Stage 2: annidamento delle tratte sotto la compagnia (array<struct>).
DROP TABLE IF EXISTS report_voli_per_carrier;
CREATE TABLE report_voli_per_carrier AS
SELECT
    op_unique_carrier,
    collect_list(named_struct(
        'origin',                  origin,
        'numero_voli',             numero_voli,
        'ritardo_minimo_arrivo',   ritardo_minimo_arrivo,
        'ritardo_massimo_arrivo',  ritardo_massimo_arrivo,
        'ritardo_medio_arrivo',    ritardo_medio_arrivo,
        'tasso_cancellazioni',     tasso_cancellazioni,
        'mesi_operativi',          mesi_operativi
    )) AS report_tratte
FROM stats_per_carrier_origin
GROUP BY op_unique_carrier;

-- Stage 3: dump dell'output finale come JSON-line su HDFS via JsonSerDe.
-- INSERT OVERWRITE DIRECTORY non propaga i nomi delle colonne al SerDe (li
-- chiamerebbe _col0/_col1). Workaround: definisco una tabella EXTERNAL con
-- LOCATION sulla dir di output e schema esplicito, poi INSERT OVERWRITE TABLE.
ADD JAR /Users/massimo/hive-4.0.1/hcatalog/share/hcatalog/hive-hcatalog-core-4.0.1.jar;

DROP TABLE IF EXISTS report_voli_per_carrier_json;
CREATE EXTERNAL TABLE report_voli_per_carrier_json (
    op_unique_carrier STRING,
    report_tratte     ARRAY<STRUCT<
        origin:                  STRING,
        numero_voli:             BIGINT,
        ritardo_minimo_arrivo:   DOUBLE,
        ritardo_massimo_arrivo:  DOUBLE,
        ritardo_medio_arrivo:    DOUBLE,
        tasso_cancellazioni:     DOUBLE,
        mesi_operativi:          ARRAY<INT>
    >>
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${hivevar:output_path}';

INSERT OVERWRITE TABLE report_voli_per_carrier_json
SELECT op_unique_carrier, report_tratte FROM report_voli_per_carrier;
