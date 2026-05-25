#!/usr/bin/env bash
# Esegue il primo job in HiveQL via beeline.
# Engine sottostante: Tez (default in Hive 4) su YARN, leggendo le tabelle da HDFS.

set -e

# Dir di questo script -> path dell'HQL da eseguire.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HQL="$SCRIPT_DIR/first-job.hql"

# Parametri I/O (gestiti dall'orchestratore bench/run_all.sh).
INPUT_PATH="${INPUT_PATH:-hdfs://localhost:9000/flight/sample/}"
OUTPUT_PATH="${OUTPUT_PATH:-hdfs://localhost:9000/flight/output/hiveql_sample}"

# Uso esplicito di $HIVE_HOME/bin/beeline: il beeline nel PATH potrebbe essere
# quello vecchio (2.x) bundlato in Spark, incompatibile con Hive 4.
HIVE_HOME="${HIVE_HOME:-/Users/massimo/hive-4.0.1}"

# Derby (metastore embedded di Hive) scrive il suo derby.log nella CWD del
# processo JVM. _JAVA_OPTIONS e' letto da OGNI JVM all'avvio, quindi il flag
# arriva alla java di beeline indipendentemente da come e' invocata.
export _JAVA_OPTIONS="-Dderby.system.home=$HIVE_HOME"

# Hive 4 NON interpola variabili (hivevar/hiveconf) dentro LOCATION/DIRECTORY:
# il parser le tratta come letterali. Aggiriamo con sed su una copia temporanea
# dell'HQL, sostituendo sia input_path che output_path.
# Cancello anche l'output dir su HDFS in advance: INSERT OVERWRITE DIRECTORY
# sovrascrive, ma e' piu' pulito ripartire dal vuoto.
HQL_TMP="$(mktemp -t firstjob_hql.XXXXXX)"
trap 'rm -f "$HQL_TMP"' EXIT
sed -e "s|\${hivevar:input_path}|${INPUT_PATH}|g" \
    -e "s|\${hivevar:output_path}|${OUTPUT_PATH}|g" \
    "$HQL" > "$HQL_TMP"

hdfs dfs -rm -r -f "$OUTPUT_PATH" 2>/dev/null || true

# Beeline in modalita' EMBEDDED (URL "jdbc:hive2://" senza host).
"$HIVE_HOME/bin/beeline" -u 'jdbc:hive2://' -n "$(whoami)" -f "$HQL_TMP"
