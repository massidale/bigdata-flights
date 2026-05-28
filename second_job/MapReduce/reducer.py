#!/usr/bin/env python3
"""Reducer: raggruppa per aeroporto e mese e produce il report finale.

Input (ordinato per chiave):
    origin,month<TAB>{"dep_delay": ..., "arr_delay": ..., ...}

Output: una riga CSV per ogni gruppo (aeroporto, mese) con i dati statistici:
    origin,month,basso_count,basso_dep_avg,basso_arr_avg,...
"""
import json
import sys
from collections import defaultdict


def emit(key, accumulators, cause_map):
    basso_count, basso_dep_sum, basso_arr_sum, \
        medio_count, medio_dep_sum, medio_arr_sum, \
        alto_count, alto_dep_sum, alto_arr_sum = accumulators

    # Calcolo delle medie
    basso_dep_avg = basso_dep_sum / basso_count if basso_count > 0 else 0.0
    basso_arr_avg = basso_arr_sum / basso_count if basso_count > 0 else 0.0
    medio_dep_avg = medio_dep_sum / medio_count if medio_count > 0 else 0.0
    medio_arr_avg = medio_arr_sum / medio_count if medio_count > 0 else 0.0
    alto_dep_avg = alto_dep_sum / alto_count if alto_count > 0 else 0.0
    alto_arr_avg = alto_arr_sum / alto_count if alto_count > 0 else 0.0

    # Sort delle cause per estrarre la Top 3
    sorted_causes = sorted(cause_map.items(), key=lambda x: x[1], reverse=True)
    top1 = sorted_causes[0][0] if len(sorted_causes) > 0 else "Nessuna"
    top2 = sorted_causes[1][0] if len(sorted_causes) > 1 else "Nessuna"
    top3 = sorted_causes[2][0] if len(sorted_causes) > 2 else "Nessuna"

    record_str = (
        f"{key},{basso_count},{basso_dep_avg:.2f},{basso_arr_avg:.2f},"
        f"{medio_count},{medio_dep_avg:.2f},{medio_arr_avg:.2f},"
        f"{alto_count},{alto_dep_avg:.2f},{alto_arr_avg:.2f},"
        f"{top1},{top2},{top3}\n"
    )
    sys.stdout.write(record_str)


def main():
    current_key = None

    # Inizializza gli accumulatori per calcolare medie e conteggi
    # [basso_cnt, basso_dep_s, basso_arr_s, medio_cnt, medio_dep_s, ...]
    accumulators = [0, 0.0, 0.0, 0, 0.0, 0.0, 0, 0.0, 0.0]
    cause_map = defaultdict(int)

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue

        try:
            key, payload_json = line.split("\t", 1)
        except ValueError:
            continue

        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError:
            continue

        if key != current_key:
            if current_key is not None:
                emit(current_key, accumulators, cause_map)
            current_key = key

            # Reset delle metriche per la nuova chiave
            accumulators = [0, 0.0, 0.0, 0, 0.0, 0.0, 0, 0.0, 0.0]
            cause_map.clear()

        # Aggiornamento delle metriche in base al payload ricevuto
        cancelled = payload.get("cancelled", 0)
        dep_delay = payload.get("dep_delay", 0.0)
        arr_delay = payload.get("arr_delay", 0.0)

        if cancelled == 0:
            if dep_delay < 15:
                accumulators[0] += 1
                accumulators[1] += dep_delay
                accumulators[2] += arr_delay
            elif dep_delay <= 60:
                accumulators[3] += 1
                accumulators[4] += dep_delay
                accumulators[5] += arr_delay
            else:
                accumulators[6] += 1
                accumulators[7] += dep_delay
                accumulators[8] += arr_delay

        canc_code = payload.get("cancellation_code", "NA")
        if cancelled == 1 and canc_code != "NA":
            cause_map[f"canc_{canc_code}"] += 1

        if payload.get("carrier_delay", 0.0) > 0: cause_map["carrier_delay"] += 1
        if payload.get("weather_delay", 0.0) > 0: cause_map["weather_delay"] += 1
        if payload.get("nas_delay", 0.0) > 0: cause_map["nas_delay"] += 1
        if payload.get("security_delay", 0.0) > 0: cause_map["security_delay"] += 1
        if payload.get("late_aircraft_delay", 0.0) > 0: cause_map["late_aircraft_delay"] += 1

    if current_key is not None:
        emit(current_key, accumulators, cause_map)


if __name__ == "__main__":
    main()