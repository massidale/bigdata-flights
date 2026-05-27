import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

# 1. SparkSession
spark = SparkSession.builder \
    .appName("AnalisiVoli") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Schema esplicito (12 colonne, header-less, output di prep/prepare.py).
# Niente inferSchema: lo schema e' fisso e definito a monte dalla prep.
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

INPUT_PATH = os.environ.get(
    "INPUT_PATH",
    "hdfs://localhost:9000/flight/bench_100/",
)

# 4. Output finale: JSON-line su HDFS (formato uniforme con gli altri 3 job).
OUTPUT_PATH = os.environ.get(
    "OUTPUT_PATH",
    "hdfs://localhost:9000/flight/output/sparksql_100",
)

df = spark.read.schema(SCHEMA).csv(INPUT_PATH, header=False)

# QUERY 1 + 2
query_1_2 = """
SELECT
  SUM(CASE WHEN dep_delay < 15 AND dep_delay > 0 THEN 1 ELSE 0 END) AS basso_count,
  AVG(CASE WHEN dep_delay < 15 AND dep_delay > 0 THEN dep_delay ELSE NULL END) AS basso_dep_delay_avg,
  AVG(CASE WHEN dep_delay < 15 AND dep_delay > 0 THEN arr_delay ELSE NULL END) AS basso_arr_delay_avg,
  SUM(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN 1 ELSE 0 END) AS medio_count,
  AVG(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN dep_delay ELSE NULL END) AS medio_dep_delay_avg,
  AVG(CASE WHEN dep_delay >= 15 AND dep_delay <= 60 THEN arr_delay ELSE NULL END) AS medio_arr_delay_avg,
  SUM(CASE WHEN dep_delay > 60 THEN 1 ELSE 0 END) AS alto_count,
  AVG(CASE WHEN dep_delay > 60 THEN dep_delay ELSE NULL END) AS alto_dep_delay_avg,
  AVG(CASE WHEN dep_delay > 60 THEN arr_delay ELSE NULL END) AS alto_arr_delay_avg
FROM flights
WHERE dep_delay IS NOT NULL AND cancelled = 0
"""

query_1_result = spark.sql(query_1_2)
query_1_result.coalesce(1).write.csv(OUTPUT_PATH + "/query_1_2", header=True, mode="overwrite")

# QUERY 3 ottimizzata (singola scansione)
query_3 = """
WITH cause_counts AS (
  SELECT
    cause,
    SUM(cnt) AS num_flights
  FROM (
    SELECT
      stack(5,
        'carrier_delay', CASE WHEN carrier_delay > 0 THEN 1 ELSE 0 END,
        'weather_delay', CASE WHEN weather_delay > 0 THEN 1 ELSE 0 END,
        'nas_delay', CASE WHEN nas_delay > 0 THEN 1 ELSE 0 END,
        'security_delay', CASE WHEN security_delay > 0 THEN 1 ELSE 0 END,
        'late_aircraft_delay', CASE WHEN late_aircraft_delay > 0 THEN 1 ELSE 0 END
      ) AS (cause, cnt)
    FROM flights
    WHERE cancelled = 0
  ) s
  GROUP BY cause
)
SELECT
  cause,
  num_flights
FROM cause_counts
ORDER BY num_flights DESC
LIMIT 3
"""

query_3_result = spark.sql(query_3)
query_3_result.coalesce(1).write.csv(OUTPUT_PATH + "/query_3", header=True, mode="overwrite")

stats_final.write.mode("overwrite").json(OUTPUT_PATH)
print(f"Output scritto in: {OUTPUT_PATH}")

spark.stop()
