#!/usr/bin/env bash
# Esegue la variante "single-query" del first_job HiveQL (first-job-pipelined.hql).
# Stesso modello del run.sh standard ma con HQL diverso.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HQL="$SCRIPT_DIR/first-job-pipelined.hql"

INPUT_PATH="${INPUT_PATH:-hdfs://localhost:9000/flight/bench_100/}"
OUTPUT_PATH="${OUTPUT_PATH:-hdfs://localhost:9000/flight/output/hiveql_pipelined_100}"

HIVE_HOME="${HIVE_HOME:-/Users/massimo/hive-4.0.1}"
export _JAVA_OPTIONS="-Dderby.system.home=$HIVE_HOME"

HQL_TMP="$(mktemp -t firstjob_pipelined_hql.XXXXXX)"
trap 'rm -f "$HQL_TMP"' EXIT
sed -e "s|\${hivevar:input_path}|${INPUT_PATH}|g" \
    -e "s|\${hivevar:output_path}|${OUTPUT_PATH}|g" \
    "$HQL" > "$HQL_TMP"

hdfs dfs -rm -r -f "$OUTPUT_PATH" 2>/dev/null || true

"$HIVE_HOME/bin/beeline" -u 'jdbc:hive2://' -n "$(whoami)" -f "$HQL_TMP"
