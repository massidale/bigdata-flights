"""Genera le size bench_50/100/200/400 a partire da /flight/clean/.

Strategia di replica controllata:
    - bench_50  : sub-sampling random al 50% (preserva distribuzioni)
    - bench_100 : copia esatta di clean
    - bench_200 : concatenazione 2x (replica naive)
    - bench_400 : concatenazione 4x (replica naive)

La replica naive 2x/4x raddoppia/quadruplica i conteggi (numero_voli) negli
output dei job, mentre MIN/MAX/AVG/COLLECT_SET restano identici (per costruzione).
Permette di misurare la scalabilita' di lettura+shuffle sul volume crescente
senza alterare la struttura semantica del problema.
"""
import os

from pyspark.sql import SparkSession


INPUT_PATH = os.environ.get(
    "INPUT_PATH",
    "hdfs://localhost:9000/flight/clean",
)
OUTPUT_BASE = os.environ.get(
    "OUTPUT_BASE",
    "hdfs://localhost:9000/flight",
)


def write_size(df, size_pct, output_path):
    """Scrive `df` come singolo file CSV header-less su HDFS."""
    df.coalesce(1).write.mode("overwrite").csv(output_path, header=False)
    print(f"[replicate] bench_{size_pct} -> {output_path}")


def main():
    spark = SparkSession.builder.appName("FlightReplicate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.csv(INPUT_PATH, header=False, inferSchema=False)
    base_count = df.count()
    print(f"[replicate] dataset clean: {base_count} righe")

    # bench_50: random sample 50%
    df_50 = df.sample(withReplacement=False, fraction=0.5, seed=42)
    write_size(df_50, 50, f"{OUTPUT_BASE}/bench_50")

    # bench_100: copia di clean
    write_size(df, 100, f"{OUTPUT_BASE}/bench_100")

    # bench_200: concat 2x (union lazy, materializzato al write)
    df_200 = df.union(df)
    write_size(df_200, 200, f"{OUTPUT_BASE}/bench_200")

    # bench_400: concat 4x
    df_400 = df_200.union(df_200)
    write_size(df_400, 400, f"{OUTPUT_BASE}/bench_400")

    print("[replicate] tutte le size generate")
    spark.stop()


if __name__ == "__main__":
    main()
