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
SCHEMA = StructType([
    StructField("op_unique_carrier",   StringType(),  True),
    StructField("origin",              StringType(),  True),
    StructField("month",               IntegerType(), True),
    StructField("dep_delay",           DoubleType(),  True),
    StructField("arr_delay",           DoubleType(),  True),
    StructField("cancelled",           DoubleType(),  True),
    StructField("cancellation_code",   StringType(),  True),
    StructField("carrier_delay",       DoubleType(),  True),
    StructField("weather_delay",       DoubleType(),  True),
    StructField("nas_delay",           DoubleType(),  True),
    StructField("security_delay",      DoubleType(),  True),
    StructField("late_aircraft_delay", DoubleType(),  True),
])

INPUT_PATH = os.environ.get(
    "INPUT_PATH",
    "hdfs://localhost:9000/flight/bench_100/",
)
df = spark.read.schema(SCHEMA).csv(INPUT_PATH, header=False)

# 3. Logica di business: groupBy (carrier, origin) -> annidamento per carrier
stats_per_carrier = df.groupBy("op_unique_carrier", "origin").agg(
    F.count("*").alias("numero_voli"),
    F.min("arr_delay").alias("ritardo_minimo_arrivo"),
    F.max("arr_delay").alias("ritardo_massimo_arrivo"),
    F.avg("arr_delay").alias("ritardo_medio_arrivo"),
    F.avg("cancelled").alias("tasso_cancellazioni"),
    F.collect_set("month").alias("mesi_operativi"),
)

stats_final = stats_per_carrier.groupBy("op_unique_carrier").agg(
    F.collect_list(
        F.struct(
            "origin", "numero_voli", "ritardo_minimo_arrivo",
            "ritardo_massimo_arrivo", "ritardo_medio_arrivo",
            "tasso_cancellazioni", "mesi_operativi",
        )
    ).alias("report_tratte")
)

# 4. Output finale: JSON-line su HDFS (formato uniforme con gli altri 3 job).
OUTPUT_PATH = os.environ.get(
    "OUTPUT_PATH",
    "hdfs://localhost:9000/flight/output/sparksql_100",
)
stats_final.write.mode("overwrite").json(OUTPUT_PATH)
print(f"Output scritto in: {OUTPUT_PATH}")

spark.stop()
