# -*- coding: utf-8 -*-
"""
Esercitazione 3.2 – Spark Core (RDD API pura)
Nessuna DataFrame/SQL API: tutto tramite sc.textFile, map, reduceByKey, ecc.
"""

import time
import sys
import csv
import io

from pyspark import SparkContext, SparkConf

# ---------------------------------------------------------------------------
# Parametri
# ---------------------------------------------------------------------------
if len(sys.argv) != 3:
    print("Uso: spark-submit esercitazione_3_2_spark_core_rdd.py <input_path> <output_path>")
    sys.exit(-1)

INPUT_PATH = sys.argv[1]
OUTPUT_PATH = sys.argv[2]

conf = SparkConf().setAppName("Task3.2_DelayReport_SparkCore")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

start_time = time.time()

# ---------------------------------------------------------------------------
# 1. Lettura e parsing del CSV
# ---------------------------------------------------------------------------
raw_rdd = sc.textFile(INPUT_PATH)

# Estrai l'intestazione e usa i nomi di colonna come chiavi
header_line = raw_rdd.first()
header = [h.strip().lower() for h in next(csv.reader(io.StringIO(header_line)))]


def parse_row(line):
    try:
        values = next(csv.reader(io.StringIO(line)))
        if len(values) != len(header):
            return None
        return dict(zip(header, values))
    except Exception:
        return None


def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def safe_int(val):
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


rows_rdd = (raw_rdd
            .filter(lambda line: line != header_line)  # rimuovi header
            .map(parse_row)
            .filter(lambda r: r is not None))  # rimuovi righe malformate

# ---------------------------------------------------------------------------
# 2. Statistiche di ritardo per fascia  (origin, month)
# ---------------------------------------------------------------------------
# Teniamo solo voli non cancellati con dep_delay valorizzato
valid_rdd = rows_rdd.filter(
    lambda r: safe_float(r.get("dep_delay")) is not None
              and safe_int(r.get("cancelled")) == 0
)


def delay_stats_mapper(r):
    """
    Emette ((origin, month), (basso_cnt, basso_dep_sum, basso_dep_cnt,
                               basso_arr_sum, basso_arr_cnt,
                               medio_cnt, medio_dep_sum, medio_dep_cnt,
                               medio_arr_sum, medio_arr_cnt,
                               alto_cnt,  alto_dep_sum,  alto_dep_cnt,
                               alto_arr_sum,  alto_arr_cnt))
    """
    origin = r.get("origin", "")
    month = r.get("month", "")
    dep_delay = safe_float(r.get("dep_delay"))
    arr_delay = safe_float(r.get("arr_delay"))  # può essere None

    # Fascia basso: 0 < dep_delay < 15
    b_cnt = 1 if 0 < dep_delay < 15 else 0
    b_dep_s = dep_delay if b_cnt else 0.0
    b_dep_c = 1 if b_cnt else 0
    b_arr_s = (arr_delay if arr_delay is not None else 0.0) if b_cnt else 0.0
    b_arr_c = (1 if arr_delay is not None else 0) if b_cnt else 0

    # Fascia medio: 15 <= dep_delay <= 60
    m_cnt = 1 if 15 <= dep_delay <= 60 else 0
    m_dep_s = dep_delay if m_cnt else 0.0
    m_dep_c = 1 if m_cnt else 0
    m_arr_s = (arr_delay if arr_delay is not None else 0.0) if m_cnt else 0.0
    m_arr_c = (1 if arr_delay is not None else 0) if m_cnt else 0

    # Fascia alto: dep_delay > 60
    a_cnt = 1 if dep_delay > 60 else 0
    a_dep_s = dep_delay if a_cnt else 0.0
    a_dep_c = 1 if a_cnt else 0
    a_arr_s = (arr_delay if arr_delay is not None else 0.0) if a_cnt else 0.0
    a_arr_c = (1 if arr_delay is not None else 0) if a_cnt else 0

    return (
        (origin, month),
        (b_cnt, b_dep_s, b_dep_c, b_arr_s, b_arr_c,
         m_cnt, m_dep_s, m_dep_c, m_arr_s, m_arr_c,
         a_cnt, a_dep_s, a_dep_c, a_arr_s, a_arr_c)
    )


def delay_stats_reducer(a, b):
    return tuple(x + y for x, y in zip(a, b))


def safe_avg(total, count):
    return total / count if count > 0 else None


stats_rdd = (valid_rdd
             .map(delay_stats_mapper)
             .reduceByKey(delay_stats_reducer)
             .mapValues(lambda v: {
    "basso_count": v[0],
    "basso_dep_delay_avg": safe_avg(v[1], v[2]),
    "basso_arr_delay_avg": safe_avg(v[3], v[4]),
    "medio_count": v[5],
    "medio_dep_delay_avg": safe_avg(v[6], v[7]),
    "medio_arr_delay_avg": safe_avg(v[8], v[9]),
    "alto_count": v[10],
    "alto_dep_delay_avg": safe_avg(v[11], v[12]),
    "alto_arr_delay_avg": safe_avg(v[13], v[14]),
}))

# ---------------------------------------------------------------------------
# 3. Top-3 cause di ritardo/cancellazione per (origin, month)
# ---------------------------------------------------------------------------
CAUSE_COLS = [
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]


def cause_mapper(r):
    """
    Per ogni riga emette coppie ((origin, month, cause), 1)
    per ogni causa con valore > 0, più la causa di cancellazione.
    """
    origin = r.get("origin", "")
    month = r.get("month", "")
    pairs = []

    for col in CAUSE_COLS:
        val = safe_float(r.get(col))
        if val is not None and val > 0:
            pairs.append(((origin, month, col), 1))

    # Causa di cancellazione
    cancelled = safe_int(r.get("cancelled"))
    canc_code = r.get("cancellation_code", "")
    if cancelled == 1 and canc_code:
        pairs.append(((origin, month, "canc_" + canc_code), 1))

    return pairs


cause_counts_rdd = (rows_rdd
                    .flatMap(cause_mapper)
                    .reduceByKey(lambda a, b: a + b))


# cause_counts_rdd: ((origin, month, cause), freq)

# Raggruppa per (origin, month) → lista di (cause, freq)
def remap_to_origin_month(item):
    (origin, month, cause), freq = item
    return ((origin, month), [(cause, freq)])


grouped_causes_rdd = (cause_counts_rdd
                      .map(remap_to_origin_month)
                      .reduceByKey(lambda a, b: a + b))


def top3_causes(cause_list):
    """Restituisce le top-3 cause ordinate per frequenza decrescente."""
    sorted_causes = sorted(cause_list, key=lambda x: x[1], reverse=True)
    result = {}
    for i, (cause, _) in enumerate(sorted_causes[:3], start=1):
        result["top_causa_{}".format(i)] = cause
    # Riempie con "Nessuna" se mancano cause
    for i in range(1, 4):
        result.setdefault("top_causa_{}".format(i), "Nessuna")
    return result


top3_rdd = grouped_causes_rdd.mapValues(top3_causes)


# top3_rdd: ((origin, month), {"top_causa_1": ..., "top_causa_2": ..., "top_causa_3": ...})

# ---------------------------------------------------------------------------
# 4. Join finale: statistiche ritardi + top-3 cause
# ---------------------------------------------------------------------------
def merge_stats_and_top3(item):
    (origin, month), (stats_dict, top3_dict) = item
    merged = {
        "origin": origin,
        "month": month,
    }
    merged.update(stats_dict)
    if top3_dict is not None:
        merged.update(top3_dict)
    else:
        merged.update({
            "top_causa_1": "Nessuna",
            "top_causa_2": "Nessuna",
            "top_causa_3": "Nessuna",
        })
    return merged


final_rdd = (stats_rdd
             .leftOuterJoin(top3_rdd)
             .map(merge_stats_and_top3)
             .sortBy(lambda r: (r["origin"], r["month"])))

# ---------------------------------------------------------------------------
# 5. Scrittura output CSV (final_report)
# ---------------------------------------------------------------------------
REPORT_COLS = [
    "origin", "month",
    "basso_count", "basso_dep_delay_avg", "basso_arr_delay_avg",
    "medio_count", "medio_dep_delay_avg", "medio_arr_delay_avg",
    "alto_count", "alto_dep_delay_avg", "alto_arr_delay_avg",
    "top_causa_1", "top_causa_2", "top_causa_3",
]


def row_to_csv(r):
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([str(r.get(c, "")) for c in REPORT_COLS])
    return buf.getvalue().rstrip("\r\n")


header_csv = ",".join(REPORT_COLS)
report_rows = final_rdd.map(row_to_csv)

header_rdd = sc.parallelize([header_csv])
output_rdd = header_rdd.union(report_rows)

output_rdd.coalesce(1).saveAsTextFile(OUTPUT_PATH + "/final_report")

# ---------------------------------------------------------------------------
# 6. Summary
# ---------------------------------------------------------------------------
end_time = time.time()
total_time_s = end_time - start_time

summary_line = (
    "total_time_s,datasetDimension,execution_mode,input_path,output_path,app_name\n"
    "{},100%,local,{},{},{}".format(total_time_s, INPUT_PATH, OUTPUT_PATH, sc.appName)
)
sc.parallelize([summary_line]).coalesce(1).saveAsTextFile(OUTPUT_PATH + "/run_summary")

sc.stop()
