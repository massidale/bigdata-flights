#!/usr/bin/env python3
import json
import sys
import io

# Forza la gestione dell'encoding UTF-8 ignorando caratteri non validi (Previene UnicodeDecodeError)
sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8', errors='ignore')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='ignore')

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