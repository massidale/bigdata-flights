#!/usr/bin/env bash
# Driver Hadoop Streaming per il job MapReduce (Task 3.2: statistiche voli e cause).
# Unico stadio MR:
#   chiave (origin, month) -> stats fasce di ritardo, medie e top 3 cause

set -e

# Jar di Hadoop Streaming: contiene la classe StreamJob che fa da "ponte"
# tra Hadoop e i tuoi script mapper/reducer in Python.
# Assicurati che la versione coincida con quella del tuo cluster (es. 3.4.3)
JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar"

# Parametrizzazione I/O (gestita dall'orchestratore bench/run_all.sh o variabili d'ambiente).
INPUT="${INPUT_PATH:-hdfs://namenode:9000/user/root/input/flight_data_clean_400}"
FINAL="${OUTPUT_PATH:-hdfs://namenode:9000/output/task3_2_mapreduce_python}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# MapReduce rifiuta di scrivere in dir gia' esistenti su HDFS.
hdfs dfs -rm -r -f "$FINAL" 2>/dev/null

# === STAGE UNICO: groupBy (origin, month) ===
echo "=== Avvio Job: groupBy (origin, month) ==="
hadoop jar "$JAR" \
    `# -files: distribuisce gli script ai NodeManager via cache distribuita di YARN.`\
    -files "$SCRIPT_DIR/mapper.py,$SCRIPT_DIR/reducer.py" \
    -mapper  "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input   "$INPUT" \
    -output  "$FINAL"

echo
echo "Job completato con successo!"
echo "Output finale (CSV): $FINAL"