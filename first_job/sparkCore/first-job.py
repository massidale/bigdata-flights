"""Primo job in Spark Core (RDD): statistiche voli per compagnia.

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

COL_CARRIER = 0
COL_ORIGIN = 1
COL_MONTH = 2
COL_ARR_DELAY = 4
COL_CANCELLED = 5
MIN_COLS = COL_CANCELLED + 1


def parse_csv_line(line):
    return next(csv.reader(io.StringIO(line)))


def extract_record(fields):
    if len(fields) < MIN_COLS:
        return None
    carrier = fields[COL_CARRIER].strip()
    origin = fields[COL_ORIGIN].strip()
    if not carrier or not origin:
        return None

    cancelled_s = fields[COL_CANCELLED].strip()
    try:
        cancelled = int(float(cancelled_s)) if cancelled_s else 0
    except ValueError:
        cancelled = 0

    arr_delay_s = fields[COL_ARR_DELAY].strip()
    try:
        d = float(arr_delay_s) if arr_delay_s else None
    except ValueError:
        d = None

    month_s = fields[COL_MONTH].strip()
    try:
        mese = int(month_s) if month_s else None
    except ValueError:
        mese = None

    mesi = frozenset([mese]) if mese is not None else frozenset()

    if d is not None:
        stat = (1, cancelled, 1, d, d, d, mesi)
    else:
        stat = (1, cancelled, 0, None, None, 0.0, mesi)

    return ((carrier, origin), stat)


def merge_stats(a, b):
    n = a[0] + b[0]
    cnc = a[1] + b[1]
    cd = a[2] + b[2]

    if a[3] is None:
        mn = b[3]
    elif b[3] is None:
        mn = a[3]
    else:
        mn = min(a[3], b[3])

    if a[4] is None:
        mx = b[4]
    elif b[4] is None:
        mx = a[4]
    else:
        mx = max(a[4], b[4])

    sm = a[5] + b[5]
    mesi = a[6] | b[6]
    return (n, cnc, cd, mn, mx, sm, mesi)


def stat_to_dict(origin, stat):
    n, cnc, cd, mn, mx, sm, mesi = stat
    return {
        "origin": origin,
        "numero_voli": n,
        "ritardo_minimo_arrivo": mn,
        "ritardo_massimo_arrivo": mx,
        "ritardo_medio_arrivo": (sm / cd) if cd > 0 else None,
        "tasso_cancellazioni": (cnc / n) if n > 0 else None,
        "mesi_operativi": sorted(mesi),
    }


def main():
    sc = SparkContext(appName="AnalisiVoli-RDD")
    sc.setLogLevel("WARN")

    lines = sc.textFile(INPUT_PATH)

    parsed = (
        lines
        .filter(lambda l: bool(l))
        .map(parse_csv_line)
        .map(extract_record)
        .filter(lambda x: x is not None)
    )

    stats_per_tratta = parsed.reduceByKey(merge_stats)

    by_carrier = stats_per_tratta.map(
        lambda kv: (kv[0][0], stat_to_dict(kv[0][1], kv[1]))
    )

    report = (
        by_carrier
        .groupByKey()
        .mapValues(list)
        .map(lambda kv: {
            "op_unique_carrier": kv[0],
            "report_tratte": kv[1],
        })
    )

    report.map(lambda r: json.dumps(r, ensure_ascii=False)).saveAsTextFile(OUTPUT_PATH)
    print(f"Output scritto in: {OUTPUT_PATH}")

    sc.stop()


if __name__ == "__main__":
    main()
