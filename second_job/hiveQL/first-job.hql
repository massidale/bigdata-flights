-- esercitazione_3_2_hive.hql

SET hive.execution.engine=mr;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

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
LOCATION 'hdfs://namenode:9000//user/root/input'
TBLPROPERTIES ("skip.header.line.count"="1");

-- QUERY 1 e 2
SELECT SUM(CASE WHEN dep_delay < 15 AND dep_delay > 0 THEN 1 ELSE 0 END)               AS basso_count,
       AVG(CASE WHEN dep_delay < 15 AND dep_delay > 0 THEN dep_delay ELSE NULL END)    AS basso_dep_delay_avg,
       AVG(CASE WHEN dep_delay < 15 AND dep_delay > 0 THEN arr_delay ELSE NULL END)    AS basso_arr_delay_avg,
       SUM(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN 1 ELSE 0 END)            AS medio_count,
       AVG(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN dep_delay ELSE NULL END) AS medio_dep_delay_avg,
       AVG(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN arr_delay ELSE NULL END) AS medio_arr_delay_avg,
       SUM(CASE WHEN dep_delay > 60 THEN 1 ELSE 0 END)                                 AS alto_count,
       AVG(CASE WHEN dep_delay > 60 THEN dep_delay ELSE NULL END)                      AS alto_dep_delay_avg,
       AVG(CASE WHEN dep_delay > 60 THEN arr_delay ELSE NULL END)                      AS alto_arr_delay_avg
FROM flights
WHERE dep_delay IS NOT NULL
  AND cancelled = 0;

-- QUERY 3: cause più frequenti
SELECT rank, cause, num_flights
FROM (SELECT cause,
             num_flights,
             RANK() OVER (ORDER BY num_flights DESC) AS rank
      FROM (SELECT 'carrier_delay' AS cause, SUM(CASE WHEN carrier_delay > 0 THEN 1 ELSE 0 END) AS num_flights
            FROM flights
            WHERE cancelled = 0

            UNION ALL

            SELECT 'weather_delay' AS cause, SUM(CASE WHEN weather_delay > 0 THEN 1 ELSE 0 END) AS num_flights
            FROM flights
            WHERE cancelled = 0

            UNION ALL

            SELECT 'nas_delay' AS cause, SUM(CASE WHEN nas_delay > 0 THEN 1 ELSE 0 END) AS num_flights
            FROM flights
            WHERE cancelled = 0

            UNION ALL

            SELECT 'security_delay' AS cause, SUM(CASE WHEN security_delay > 0 THEN 1 ELSE 0 END) AS num_flights
            FROM flights
            WHERE cancelled = 0

            UNION ALL

            SELECT 'late_aircraft_delay'                                    AS cause,
                   SUM(CASE WHEN late_aircraft_delay > 0 THEN 1 ELSE 0 END) AS num_flights
            FROM flights
            WHERE cancelled = 0) counts) ranked
WHERE rank <= 3
ORDER BY rank;