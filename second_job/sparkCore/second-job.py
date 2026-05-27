"""Secondo job in Spark Core (RDD): statistiche voli per compagnia.

Pipeline:
    Stage 1: (carrier, origin) -> stats per tratta
    Stage 2: carrier            -> array annidato di tratte

Input atteso: /flight/bench_<size>/ (output di prep+replicate, header-less, 12 colonne).
Schema (header-less, da prep/prepare.py):
    0  op_unique_carrier      <-- usato
    1  origin                 <-- usato
    2  month                  <-- usato
    3  dep_delay
    4  arr_delay              <-- usato
    5  cancelled              <-- usato
    6..11 *_code/*_delay
"""
import csv
import io
import json
import os

from pyspark import SparkContext

INPUT_PATH = os.environ.get(
    "INPUT_PATH",
    "hdfs://localhost:9000/flight/bench_100/",
)
OUTPUT_PATH = os.environ.get(
    "OUTPUT_PATH",
    "hdfs://localhost:9000/flight/output/sparkcore_100",
)

df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)
df = df.repartition(8)  # Repartition per migliorare le performance

flights = df.filter((F.col("dep_delay").isNotNull()) & (F.col("cancelled") == 0))

# QUERY 1: numero di voli appartenenti a tre fasce di ritardo in partenza
# QUERY 2: per ciascuna fascia, il ritardo medio in partenza e il ritardo medio in arrivo
query_1_2_result = flights.agg(
    F.sum(F.when((F.col("dep_delay") > 0) & (F.col("dep_delay") < 15), 1).otherwise(0)).alias("basso_count"),
    F.avg(F.when((F.col("dep_delay") > 0) & (F.col("dep_delay") < 15), F.col("dep_delay"))).alias(
        "basso_dep_delay_avg"),
    F.avg(F.when((F.col("dep_delay") > 0) & (F.col("dep_delay") < 15), F.col("arr_delay"))).alias(
        "basso_arr_delay_avg"),
    F.sum(F.when((F.col("dep_delay") >= 15) & (F.col("dep_delay") <= 60), 1).otherwise(0)).alias("medio_count"),
    F.avg(F.when((F.col("dep_delay") >= 15) & (F.col("dep_delay") <= 60), F.col("dep_delay"))).alias(
        "medio_dep_delay_avg"),
    F.avg(F.when((F.col("dep_delay") >= 15) & (F.col("dep_delay") <= 60), F.col("arr_delay"))).alias(
        "medio_arr_delay_avg"),
    F.sum(F.when(F.col("dep_delay") > 60, 1).otherwise(0)).alias("alto_count"),
    F.avg(F.when(F.col("dep_delay") > 60, F.col("dep_delay"))).alias("alto_dep_delay_avg"),
    F.avg(F.when(F.col("dep_delay") > 60, F.col("arr_delay"))).alias("alto_arr_delay_avg")
)

query_1_2_result.repartition(1).write.csv(OUTPUT_PATH + "/query_1_2", header=True, mode="overwrite")

# QUERY 3: top 3 cause di ritardo più frequenti nel dataset
cause_counts = [
    flights.agg(F.sum(F.when(F.col("carrier_delay") > 0, 1).otherwise(0)).alias("num_flights"))
    .withColumn("cause", F.lit("carrier_delay")),
    flights.agg(F.sum(F.when(F.col("weather_delay") > 0, 1).otherwise(0)).alias("num_flights"))
    .withColumn("cause", F.lit("weather_delay")),
    flights.agg(F.sum(F.when(F.col("nas_delay") > 0, 1).otherwise(0)).alias("num_flights"))
    .withColumn("cause", F.lit("nas_delay")),
    flights.agg(F.sum(F.when(F.col("security_delay") > 0, 1).otherwise(0)).alias("num_flights"))
    .withColumn("cause", F.lit("security_delay")),
    flights.agg(F.sum(F.when(F.col("late_aircraft_delay") > 0, 1).otherwise(0)).alias("num_flights"))
    .withColumn("cause", F.lit("late_aircraft_delay"))
]

all_causes = cause_counts[0]
for cause_df in cause_counts[1:]:
    all_causes = all_causes.unionByName(cause_df)

ranking_window = Window.orderBy(F.col("num_flights").desc())
query_3_result = (
    all_causes
    .withColumn("rank", F.rank().over(ranking_window))
    .select("rank", "cause", "num_flights")
    .filter(F.col("rank") <= 3)
    .orderBy("rank")
)

query_3_result.repartition(1).write.csv(OUTPUT_PATH + "/query_3", header=True, mode="overwrite")

end_time = time.time()
total_time_s = end_time - start_time

summary_row = [{
    "total_time_s": total_time_s,
    "datasetDimension": "100%",
    "execution_mode": "local",
    "input_path": INPUT_PATH,
    "output_path": OUTPUT_PATH,
    "repartition_num": 8,
    "app_name": spark.sparkContext.appName,
}]

summary_df = spark.createDataFrame(summary_row)
summary_df.coalesce(1).write.csv(
    OUTPUT_PATH + "/run_summary",
    header=True,
    mode="overwrite",
)

if __name__ == "__main__":
    main()
