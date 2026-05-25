#!/usr/bin/env python3
"""Stage 1 reducer: aggrega per (carrier, origin) e produce le stat finali della tratta.

Input atteso (gia' ordinato da Hadoop sulla chiave a 2 campi):
    carrier<TAB>origin<TAB>{"n":..., "canc":..., "cd":..., "min":..., "max":..., "sum":..., "mesi":[...]}

Output:
    carrier<TAB>origin<TAB>{"numero_voli":..., "ritardo_minimo_arrivo":...,
                            "ritardo_massimo_arrivo":..., "ritardo_medio_arrivo":...,
                            "tasso_cancellazioni":..., "mesi_operativi":[...]}
"""
import json
import sys


def emit(carrier, origin, agg):
    n, canc, cd, mn, mx, sm, mesi = agg
    payload = {
        "numero_voli": n,
        "ritardo_minimo_arrivo": mn,
        "ritardo_massimo_arrivo": mx,
        "ritardo_medio_arrivo": (sm / cd) if cd > 0 else None,
        "tasso_cancellazioni": (canc / n) if n > 0 else None,
        "mesi_operativi": sorted(mesi),
    }
    sys.stdout.write(f"{carrier}\t{origin}\t{json.dumps(payload, ensure_ascii=False)}\n")


def main():
    current_key = None
    agg = None

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        try:
            carrier, origin, payload_json = line.split("\t", 2)
        except ValueError:
            continue
        try:
            p = json.loads(payload_json)
        except json.JSONDecodeError:
            continue

        key = (carrier, origin)
        if key != current_key:
            if current_key is not None:
                emit(current_key[0], current_key[1], agg)
            current_key = key
            agg = [0, 0, 0, None, None, 0.0, set()]

        agg[0] += p["n"]
        agg[1] += p["canc"]
        agg[2] += p["cd"]
        if p["min"] is not None:
            agg[3] = p["min"] if agg[3] is None else min(agg[3], p["min"])
        if p["max"] is not None:
            agg[4] = p["max"] if agg[4] is None else max(agg[4], p["max"])
        agg[5] += p["sum"]
        agg[6].update(p["mesi"])

    if current_key is not None:
        emit(current_key[0], current_key[1], agg)


if __name__ == "__main__":
    main()
