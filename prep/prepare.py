"""Data prep pipeline (Spark): produce /flight/clean/ a partire dal full CSV.

Operazioni:
    1. Selezione delle 12 colonne effettivamente usate dai 3 job dell'analisi
       (3.1 statistiche compagnie, 3.2 ritardi per aeroporto/mese,
        3.3 ranking compagnia-aeroporto).
    2. Filtro righe con op_unique_carrier o origin null/vuoti
       (sono record inutilizzabili per qualunque analisi).
    3. Output: CSV header-less, cosi' i job a valle assumono schema fisso
       senza bisogno di header-detection.

Schema clean (in ordine, header-less):
    0  op_unique_carrier      STRING
    1  origin                 STRING
    2  month                  INT
    3  dep_delay              DOUBLE
    4  arr_delay              DOUBLE
    5  cancelled              DOUBLE
    6  cancellation_code      STRING
    7  carrier_delay          DOUBLE
    8  weather_delay          DOUBLE
    9  nas_delay              DOUBLE
    10 security_delay         DOUBLE
    11 late_aircraft_delay    DOUBLE
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


INPUT_PATH = os.environ.get(
    "INPUT_PATH",
    "hdfs://localhost:9000/flight/flight_data_2024.csv",
)
OUTPUT_PATH = os.environ.get(
    "OUTPUT_PATH",
    "hdfs://localhost:9000/flight/clean",
)

WANTED_COLS = [
    "op_unique_carrier",
    "origin",
    "month",
    "dep_delay",
    "arr_delay",
    "cancelled",
    "cancellation_code",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]


def main():
    spark = SparkSession.builder.appName("FlightPrep").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.csv(INPUT_PATH, header=True, inferSchema=False)

    print(f"[prep] input righe: {df.count()}")

    df_clean = (
        df.select(*WANTED_COLS)
          .filter(
              F.col("op_unique_carrier").isNotNull()
              & (F.col("op_unique_carrier") != "")
              & F.col("origin").isNotNull()
              & (F.col("origin") != "")
          )
    )

    n_clean = df_clean.count()
    print(f"[prep] output righe (post-filter): {n_clean}")

    # Header-less CSV. coalesce(1) per avere un singolo file di output
    # (semplifica la replica successiva e la lettura dei job).
    df_clean.coalesce(1).write.mode("overwrite").csv(OUTPUT_PATH, header=False)

    print(f"[prep] scritto in {OUTPUT_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
