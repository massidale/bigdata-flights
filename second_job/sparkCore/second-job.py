# -*- coding: utf-8 -*-
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Task3.2_DelayReport_SparkCore_Final").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

INPUT_PATH = "hdfs://namenode:9000//user/root/input/flight_data_clean_50"
OUTPUT_PATH = "hdfs://namenode:9000//output/task3_2_sparkcore_final"

start_time = time.time()

df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)
repartition_num = 8
df = df.repartition(repartition_num)

df.cache()
df.count()
flights_valid_delay = df.filter((F.col("dep_delay").isNotNull()) & (F.col("cancelled") == 0))

stats_ritardi_df = flights_valid_delay.groupBy("origin", "month").agg(
    F.sum(F.when(F.col("dep_delay") < 15, 1).otherwise(0)).alias("basso_count"),
    F.avg(F.when(F.col("dep_delay") < 15, F.col("dep_delay"))).alias("basso_dep_delay_avg"),
    F.avg(F.when(F.col("dep_delay") < 15, F.col("arr_delay"))).alias("basso_arr_delay_avg"),

    F.sum(F.when((F.col("dep_delay") >= 15) & (F.col("dep_delay") <= 60), 1).otherwise(0)).alias("medio_count"),
    F.avg(F.when((F.col("dep_delay") >= 15) & (F.col("dep_delay") <= 60), F.col("dep_delay"))).alias(
        "medio_dep_delay_avg"),
    F.avg(F.when((F.col("dep_delay") >= 15) & (F.col("dep_delay") <= 60), F.col("arr_delay"))).alias(
        "medio_arr_delay_avg"),

    F.sum(F.when(F.col("dep_delay") > 60, 1).otherwise(0)).alias("alto_count"),
    F.avg(F.when(F.col("dep_delay") > 60, F.col("dep_delay"))).alias("alto_dep_delay_avg"),
    F.avg(F.when(F.col("dep_delay") > 60, F.col("arr_delay"))).alias("alto_arr_delay_avg")
)

exploded_causes_df = df.select(
    "origin", "month",
    F.explode(F.array(
        F.struct(F.lit("carrier_delay").alias("cause"),
                 F.when(F.col("carrier_delay") > 0, 1).otherwise(0).alias("cnt")),
        F.struct(F.lit("weather_delay").alias("cause"),
                 F.when(F.col("weather_delay") > 0, 1).otherwise(0).alias("cnt")),
        F.struct(F.lit("nas_delay").alias("cause"), F.when(F.col("nas_delay") > 0, 1).otherwise(0).alias("cnt")),
        F.struct(F.lit("security_delay").alias("cause"),
                 F.when(F.col("security_delay") > 0, 1).otherwise(0).alias("cnt")),
        F.struct(F.lit("late_aircraft_delay").alias("cause"),
                 F.when(F.col("late_aircraft_delay") > 0, 1).otherwise(0).alias("cnt")),
        F.struct(F.concat(F.lit("canc_"), F.col("cancellation_code")).alias("cause"),
                 F.when((F.col("cancelled") == 1) & (F.col("cancellation_code").isNotNull()), 1).otherwise(0).alias(
                     "cnt"))
    )).alias("cause_struct")
).select(
    "origin", "month",
    F.col("cause_struct.cause").alias("cause"),
    F.col("cause_struct.cnt").alias("cnt")
).filter(F.col("cnt") > 0)

cause_counts_df = exploded_causes_df.groupBy("origin", "month", "cause").count().withColumnRenamed("count", "freq")

ranking_window = Window.partitionBy("origin", "month").orderBy(F.col("freq").desc())

ranked_causes_df = cause_counts_df.withColumn("rnk", F.row_number().over(ranking_window)).filter(F.col("rnk") <= 3)

top_3_causes_columns = ranked_causes_df.groupBy("origin", "month").agg(
    F.max(F.when(F.col("rnk") == 1, F.col("cause"))).alias("top_causa_1"),
    F.max(F.when(F.col("rnk") == 2, F.col("cause"))).alias("top_causa_2"),
    F.max(F.when(F.col("rnk") == 3, F.col("cause"))).alias("top_causa_3")
)

final_report_df = stats_ritardi_df.join(top_3_causes_columns, on=["origin", "month"], how="left") \
    .na.fill("Nessuna", ["top_causa_1", "top_causa_2", "top_causa_3"]) \
    .orderBy("origin", "month")

final_report_df.coalesce(1).write.csv(OUTPUT_PATH + "/final_report", header=True, mode="overwrite")

end_time = time.time()
total_time_s = end_time - start_time

summary_row = [{
    "total_time_s": total_time_s,
    "datasetDimension": "100%",
    "execution_mode": "local",
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

spark.stop()