#!/usr/bin/env bash
# Driver Hadoop Streaming per il primo job MapReduce (statistiche voli per compagnia).
# Due stadi MR concatenati:
#   stage 1: chiave (carrier, origin) -> stats per tratta (count, min/max/avg ritardo, ...)
#   stage 2: chiave  carrier          -> array annidato di tratte per compagnia

set -e

# Jar di Hadoop Streaming: contiene la classe StreamJob che fa da "ponte"
# tra Hadoop e i tuoi script mapper/reducer in Python.
JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar"

# Parametrizzazione I/O (gestita dall'orchestratore bench/run_all.sh).
INPUT="${INPUT_PATH:-hdfs://localhost:9000/flight/sample/}"
FINAL="${OUTPUT_PATH:-hdfs://localhost:9000/flight/output/mapreduce_sample}"

# Dir intermedia stage1->stage2. Derivata da OUTPUT_PATH per non pestarsi i piedi
# con altri run paralleli su size diverse. NIENTE underscore leading: Hadoop
# FileInputFormat li tratta come hidden e stage2 non vedrebbe l'input.
INTER="${FINAL}_tmp_stage1"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# MapReduce rifiuta di scrivere in dir gia' esistenti su HDFS.
hdfs dfs -rm -r -f "$INTER" "$FINAL" 2>/dev/null

# === STAGE 1: groupBy (carrier, origin) ===
echo "=== Stage 1: groupBy (carrier, origin) ==="
hadoop jar "$JAR" \
    `# stream.num.map.output.key.fields=2 -> chiave composta sui primi 2 campi del`\
    `# tab-separated output del mapper. Senza, Hadoop partiziona solo sul 1' campo.`\
    -D stream.num.map.output.key.fields=2 \
    -D mapreduce.map.output.key.field.separator=$'\t' \
    `# -files: distribuisce gli script ai NodeManager via cache distribuita di YARN.`\
    -files "$SCRIPT_DIR/stage1_mapper.py,$SCRIPT_DIR/stage1_reducer.py" \
    -mapper  "python3 stage1_mapper.py" \
    -reducer "python3 stage1_reducer.py" \
    -input   "$INPUT" \
    -output  "$INTER"

# === STAGE 2: groupBy carrier (annidamento tratte, output JSON-line) ===
echo "=== Stage 2: groupBy carrier (annidamento tratte) ==="
hadoop jar "$JAR" \
    -files "$SCRIPT_DIR/stage2_mapper.py,$SCRIPT_DIR/stage2_reducer.py" \
    -mapper  "python3 stage2_mapper.py" \
    -reducer "python3 stage2_reducer.py" \
    -input   "$INTER" \
    -output  "$FINAL"

# Cleanup della dir intermedia: a quel punto stage2 ha gia' letto i dati.
hdfs dfs -rm -r -f "$INTER" 2>/dev/null

echo
echo "Output finale (JSON-line): $FINAL"
