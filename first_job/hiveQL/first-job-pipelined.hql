-- Variante "single-query" del first_job in HiveQL.
-- Niente CTAS intermedi: i 2 GROUP BY sono annidati in un'unica statement,
-- cosi' Calcite vede l'intero piano e Tez puo' eseguirlo come un singolo
-- DAG con i 2 vertex GROUP BY in pipeline (no materializzazione intermedia).
--
-- Stesso output finale (JSON-line via JsonSerDe) dello script CTAS-based,
-- cosi' i due bench sono direttamente confrontabili.

ADD JAR /Users/massimo/hive-4.0.1/hcatalog/share/hcatalog/hive-hcatalog-core-4.0.1.jar;

CREATE DATABASE IF NOT EXISTS flight;
USE flight;

-- Tabella esterna sul CSV clean (header-less, 12 colonne).
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

-- Tabella esterna di destinazione (JSON-line via JsonSerDe).
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

-- Unica statement: sub-query con il primo GROUP BY, GROUP BY esterno per annidamento.
INSERT OVERWRITE TABLE report_voli_per_carrier_json
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
FROM (
    SELECT
        op_unique_carrier,
        origin,
        COUNT(*)                                     AS numero_voli,
        MIN(CAST(arr_delay AS DOUBLE))               AS ritardo_minimo_arrivo,
        MAX(CAST(arr_delay AS DOUBLE))               AS ritardo_massimo_arrivo,
        AVG(CAST(arr_delay AS DOUBLE))               AS ritardo_medio_arrivo,
        AVG(CAST(cancelled AS DOUBLE))               AS tasso_cancellazioni,
        sort_array(collect_set(CAST(month AS INT)))  AS mesi_operativi
    FROM flight_raw
    GROUP BY op_unique_carrier, origin
) AS stats
GROUP BY op_unique_carrier;
