#!/usr/bin/env python3
"""Stage 2 reducer: raggruppa per compagnia e produce il record JSON finale.

Input (ordinato per carrier):
    carrier<TAB>{stat tratta}

Output: una riga JSON per compagnia, formato equivalente a quello prodotto dal job Spark
in first-job.py:
    {"op_unique_carrier": "...", "report_tratte": [ {stat tratta}, ... ]}
"""
import json
import sys


def emit(carrier, tratte):
    record = {
        "op_unique_carrier": carrier,
        "report_tratte": tratte,
    }
    sys.stdout.write(json.dumps(record, ensure_ascii=False) + "\n")


def main():
    current_carrier = None
    tratte = []

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        try:
            carrier, payload_json = line.split("\t", 1)
        except ValueError:
            continue
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError:
            continue

        if carrier != current_carrier:
            if current_carrier is not None:
                emit(current_carrier, tratte)
            current_carrier = carrier
            tratte = []

        tratte.append(payload)

    if current_carrier is not None:
        emit(current_carrier, tratte)


if __name__ == "__main__":
    main()
