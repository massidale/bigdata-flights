#!/usr/bin/env python3
"""Mapper: estrae l'aeroporto di partenza e il mese come chiave.

Input (CSV):
    origin,month,dep_delay,arr_delay,cancelled,cancellation_code,carrier_delay,...

Output:
    origin,month<TAB>{"dep_delay": ..., "arr_delay": ..., "cancelled": ..., "cancellation_code": ..., ...}
"""
import json
import sys


def main():
    out = sys.stdout
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line or line.startswith("origin"):
            continue

        tokens = line.split(",")
        if len(tokens) < 11:
            continue

        origin = tokens[0]
        month = tokens[1]

        try:
            # Creazione del payload in un dizionario
            payload = {
                "dep_delay": float(tokens[2]) if tokens[2] else 0.0,
                "arr_delay": float(tokens[3]) if tokens[3] else 0.0,
                "cancelled": int(tokens[4]) if tokens[4] else 0,
                "cancellation_code": tokens[5] if tokens[5] else "NA",
                "carrier_delay": float(tokens[6]) if tokens[6] else 0.0,
                "weather_delay": float(tokens[7]) if tokens[7] else 0.0,
                "nas_delay": float(tokens[8]) if tokens[8] else 0.0,
                "security_delay": float(tokens[9]) if tokens[9] else 0.0,
                "late_aircraft_delay": float(tokens[10]) if tokens[10] else 0.0
            }
        except ValueError:
            continue

        key = f"{origin},{month}"
        out.write(f"{key}\t{json.dumps(payload, ensure_ascii=False)}\n")


if __name__ == "__main__":
    main()