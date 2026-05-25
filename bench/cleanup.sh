#!/usr/bin/env bash
# Cleanup degli artefatti dei job (HDFS output dirs + tabelle Hive + log).
# Lascia intatti i dataset di input (/flight/clean/, /flight/bench_*/, /flight/full/),
# i CSV ricevuti dal client e le metriche/snippet locali.
#
# Uso:
#   bench/cleanup.sh                   # tutti i job, tutte le size
#   bench/cleanup.sh <size>            # tutti i job, una size
# Esempi:
#   bench/cleanup.sh 50
#   bench/cleanup.sh

set -e

SIZE="${1:-}"

echo "=== Cleanup HDFS /flight/output/ ==="
if [ -n "$SIZE" ]; then
    for fw in mapreduce sparkcore sparksql hiveql; do
        path="/flight/output/${fw}_${SIZE}"
        echo "  rm $path"
        hdfs dfs -rm -r -f "$path"               2>/dev/null || true
        hdfs dfs -rm -r -f "${path}_tmp_stage1"  2>/dev/null || true
    done
else
    hdfs dfs -rm -r -f /flight/output 2>/dev/null || true
fi

# Tabelle Hive del database 'flight'. DROP rimuove anche i file dal warehouse
# per le tabelle managed (stats_per_carrier_origin, report_voli_per_carrier).
# flight_raw e' EXTERNAL: i CSV sotto LOCATION restano intatti.
echo "=== Cleanup tabelle Hive 'flight' ==="
HIVE_HOME="${HIVE_HOME:-/Users/massimo/hive-4.0.1}"
export _JAVA_OPTIONS="-Dderby.system.home=$HIVE_HOME"
"$HIVE_HOME/bin/beeline" -u 'jdbc:hive2://' -n "$(whoami)" --silent=true -e "
DROP TABLE IF EXISTS flight.report_voli_per_carrier_json;
DROP TABLE IF EXISTS flight.report_voli_per_carrier;
DROP TABLE IF EXISTS flight.stats_per_carrier_origin;
DROP TABLE IF EXISTS flight.flight_raw;
" 2>/dev/null || true

echo "=== Cleanup log locali /tmp/ ==="
rm -f /tmp/first_job_*.log /tmp/second_job_*.log

echo "=== Cleanup completato ==="
