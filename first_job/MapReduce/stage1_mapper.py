#!/usr/bin/env python3
"""Stage 1 mapper: in-mapper combiner per chiave (carrier, origin).

Legge righe CSV (formato /flight/clean/) da stdin ed emette su stdout righe:
    carrier<TAB>origin<TAB>{"n":..., "canc":..., "cd":..., "min":..., "max":..., "sum":..., "mesi":[...]}

Schema atteso (output di prep/prepare.py, 12 colonne, header-less):
    0  op_unique_carrier     STRING   <-- usato
    1  origin                STRING   <-- usato
    2  month                 INT      <-- usato
    3  dep_delay             DOUBLE
    4  arr_delay             DOUBLE   <-- usato
    5  cancelled             DOUBLE   <-- usato
    6  cancellation_code     STRING
    7..11 *_delay            DOUBLE

Note:
    - Hadoop Streaming TextInputFormat passa al mapper "offset<TAB>linea<LF>".
      Strippiamo l'offset se rilevato (TAB prima della 1a virgola).
    - Niente header-detection: il prep ha gia' rimosso l'header. Eventuali split
      sui blocchi HDFS non rompono nulla.
"""
import csv
import io
import json
import sys

COL_CARRIER = 0
COL_ORIGIN = 1
COL_MONTH = 2
COL_ARR_DELAY = 4
COL_CANCELLED = 5

MIN_COLS = COL_CANCELLED + 1


def strip_streaming_key(raw):
    """Hadoop Streaming prepende l'offset come key (offset<TAB>value).
    Rimuovilo se c'e' un TAB prima della 1a virgola."""
    tab_idx = raw.find("\t")
    if tab_idx < 0:
        return raw
    comma_idx = raw.find(",")
    if comma_idx < 0 or tab_idx < comma_idx:
        return raw[tab_idx + 1:]
    return raw


def main():
    acc = {}

    for raw in sys.stdin:
        raw = raw.rstrip("\n").rstrip("\r")
        if not raw:
            continue
        raw = strip_streaming_key(raw)
        try:
            row = next(csv.reader(io.StringIO(raw)))
        except (StopIteration, csv.Error):
            continue

        if len(row) < MIN_COLS:
            continue

        carrier = row[COL_CARRIER].strip()
        origin = row[COL_ORIGIN].strip()
        if not carrier or not origin:
            continue

        cancelled_s = row[COL_CANCELLED].strip()
        try:
            cancelled = int(float(cancelled_s)) if cancelled_s else 0
        except ValueError:
            cancelled = 0

        arr_delay_s = row[COL_ARR_DELAY].strip()
        try:
            d = float(arr_delay_s) if arr_delay_s else None
        except ValueError:
            d = None

        month_s = row[COL_MONTH].strip()
        try:
            mese = int(month_s) if month_s else None
        except ValueError:
            mese = None

        key = (carrier, origin)
        rec = acc.get(key)
        if rec is None:
            # [n, canc, count_delay, min_delay, max_delay, sum_delay, set_mesi]
            rec = [0, 0, 0, None, None, 0.0, set()]
            acc[key] = rec

        rec[0] += 1
        rec[1] += cancelled

        if d is not None:
            rec[2] += 1
            rec[3] = d if rec[3] is None else (d if d < rec[3] else rec[3])
            rec[4] = d if rec[4] is None else (d if d > rec[4] else rec[4])
            rec[5] += d

        if mese is not None:
            rec[6].add(mese)

    out = sys.stdout
    for (carrier, origin), rec in acc.items():
        payload = {
            "n": rec[0],
            "canc": rec[1],
            "cd": rec[2],
            "min": rec[3],
            "max": rec[4],
            "sum": rec[5],
            "mesi": sorted(rec[6]),
        }
        out.write(f"{carrier}\t{origin}\t{json.dumps(payload, ensure_ascii=False)}\n")


if __name__ == "__main__":
    main()
