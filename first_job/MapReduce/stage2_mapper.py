#!/usr/bin/env python3
"""Stage 2 mapper: re-chiavizza l'output dello stage 1 per la sola compagnia.

Input:
    carrier<TAB>origin<TAB>{stat tratta}

Output:
    carrier<TAB>{stat tratta + "origin": <origin>}
"""
import json
import sys


def main():
    out = sys.stdout
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        try:
            carrier, origin, payload_json = line.split("\t", 2)
        except ValueError:
            continue
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError:
            continue

        payload["origin"] = origin
        out.write(f"{carrier}\t{json.dumps(payload, ensure_ascii=False)}\n")


if __name__ == "__main__":
    main()
