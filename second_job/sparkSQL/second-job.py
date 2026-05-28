# -*- coding: utf-8 -*-
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

schema = StructType([
    StructField("origin", StringType(), True),
    StructField("month", IntegerType(), True),
    StructField("dep_delay", DoubleType(), True),
    StructField("arr_delay", DoubleType(), True),
    StructField("cancelled", IntegerType(), True),
    StructField("cancellation_code", StringType(), True),
    StructField("carrier_delay", DoubleType(), True),
    StructField("weather_delay", DoubleType(), True),
    StructField("nas_delay", DoubleType(), True),
    StructField("security_delay", DoubleType(), True),
    StructField("late_aircraft_delay", DoubleType(), True),
])

spark = (
    SparkSession.builder
    .appName("Task3.2_DelayReport_Final_SparkSQL")
    .config("spark.sql.shuffle.partitions", "24")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

INPUT_PATH = "hdfs://namenode:9000//user/root/input/flight_data_clean_400"
OUTPUT_PATH = "hdfs://namenode:9000//output/task3_2_final_report"

start_time = time.time()

df = (
    spark.read
    .schema(schema)
    .option("header", "true")
    .csv(INPUT_PATH)
)

repartition_num = 24
df = df.repartition(repartition_num)

df.cache()
df.count()

df.createOrReplaceTempView("flights")

query_report_completo = """
                        WITH stats_ritardi AS (
                            -- STAGE 1 & 2: Classificazione fasce e aggregazione metriche (a) e (b) per aeroporto e mese
                            SELECT origin,
                            month \
                           , SUM (CASE WHEN dep_delay \
                           < 15 THEN 1 ELSE 0 END) AS basso_count \
                           , AVG (CASE WHEN dep_delay \
                           < 15 THEN dep_delay ELSE NULL END) AS basso_dep_delay_avg \
                           , AVG (CASE WHEN dep_delay \
                           < 15 THEN arr_delay ELSE NULL END) AS basso_arr_delay_avg \
                           , SUM (CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN 1 ELSE 0 END) AS medio_count \
                           , AVG (CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN dep_delay ELSE NULL END) AS medio_dep_delay_avg \
                           , AVG (CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN arr_delay ELSE NULL END) AS medio_arr_delay_avg \
                           , SUM (CASE WHEN dep_delay \
                           > 60 THEN 1 ELSE 0 END) AS alto_count \
                           , AVG (CASE WHEN dep_delay \
                           > 60 THEN dep_delay ELSE NULL END) AS alto_dep_delay_avg \
                           , AVG (CASE WHEN dep_delay \
                           > 60 THEN arr_delay ELSE NULL END) AS alto_arr_delay_avg
                        FROM flights
                        WHERE cancelled = 0 AND dep_delay IS NOT NULL
                        GROUP BY origin, month
                            ), \
                            cause_unpivoted AS (
                        -- STAGE 3: Unpivot delle cause sia di ritardo che di cancellazione tramite stack()
                        SELECT
                            origin, month, cause
                        FROM flights
                            LATERAL VIEW stack(6, 'carrier_delay', CASE WHEN carrier_delay > 0 THEN 1 ELSE 0 END, 'weather_delay', CASE WHEN weather_delay > 0 THEN 1 ELSE 0 END, 'nas_delay', CASE WHEN nas_delay > 0 THEN 1 ELSE 0 END, 'security_delay', CASE WHEN security_delay > 0 THEN 1 ELSE 0 END, 'late_aircraft_delay', CASE WHEN late_aircraft_delay > 0 THEN 1 ELSE 0 END, CONCAT('canc_', cancellation_code), CASE WHEN cancelled = 1 AND cancellation_code IS NOT NULL THEN 1 ELSE 0 END
                            ) s AS (cause, cnt)
                        WHERE cnt \
                            > 0 \
                          AND cause IS NOT NULL
                            ) \
                            , cause_conteggiate AS (
                        -- Calcolo frequenza di ogni causa per specifico aeroporto e mese
                        SELECT
                            origin, month, cause, COUNT (*) AS freq
                        FROM cause_unpivoted
                        GROUP BY origin, month, cause
                            ), \
                            cause_classificate AS (
                        -- Assegnazione del ranking locale basato sulla frequenza
                        SELECT
                            origin, month, cause, ROW_NUMBER() OVER (PARTITION BY origin, month ORDER BY freq DESC) AS rnk
                        FROM cause_conteggiate
                            ), top_3_cause AS (
                        -- Pivot condizionale per estrarre esattamente le prime 3 cause su singola riga
                        SELECT
                            origin, month, MAX (CASE WHEN rnk = 1 THEN cause ELSE NULL END) AS top_causa_1, MAX (CASE WHEN rnk = 2 THEN cause ELSE NULL END) AS top_causa_2, MAX (CASE WHEN rnk = 3 THEN cause ELSE NULL END) AS top_causa_3
                        FROM cause_classificate
                        WHERE rnk <= 3
                        GROUP BY origin, month
                            )

                        SELECT r.origin, \
                               r.month, \
                               r.basso_count, \
                               r.basso_dep_delay_avg, \
                               r.basso_arr_delay_avg, \
                               r.medio_count, \
                               r.medio_dep_delay_avg, \
                               r.medio_arr_delay_avg, \
                               r.alto_count, \
                               r.alto_dep_delay_avg, \
                               r.alto_arr_delay_avg, \
                               COALESCE(c.top_causa_1, 'Nessuna') AS top_causa_1, \
                               COALESCE(c.top_causa_2, 'Nessuna') AS top_causa_2, \
                               COALESCE(c.top_causa_3, 'Nessuna') AS top_causa_3
                        FROM stats_ritardi r
                                 LEFT JOIN top_3_cause c ON r.origin = c.origin AND r.month = c.month
                        ORDER BY r.origin, r.month \
                        """

report_df = spark.sql(query_report_completo)
report_df.coalesce(1).write.csv(OUTPUT_PATH + "/final_report", header=True, mode="overwrite")

end_time = time.time()
total_time_s = end_time - start_time

summary_row = [{
    "total_time_s": total_time_s,
    "datasetDimension": "100%",
    "execution_mode": "docker-hdfs",
    "input_path": INPUT_PATH,
    "output_path": OUTPUT_PATH,
    "repartition_num": repartition_num,
    "app_name": spark.sparkContext.appName,
}]

summary_df = spark.createDataFrame(summary_row)
summary_df.coalesce(1).write.csv(
    OUTPUT_PATH + "/run_summary",
    header=True,
    mode="overwrite",
)