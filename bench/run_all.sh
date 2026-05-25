#!/usr/bin/env bash
# Orchestratore: lancia i 4 framework di <job> su una <size> per <repeats> volte.
# Misura wall-clock, salva log, estrae prime 10 righe dell'output JSON.
#
# Uso:
#   bench/run_all.sh [job] [size] [repeats]
#     job     : first_job (default) | second_job (futuro)
#     size    : 50 | 100 | 200 | 400 (default 100). Input HDFS: /flight/bench_<size>/.
#     repeats : numero esecuzioni (default 1). Standard per bench: 3, scarta la prima.
#
# Esempi:
#   bench/run_all.sh                       # first_job, size=100, repeats=1
#   bench/run_all.sh first_job 50 3        # first_job, size=50, 3 run
#   bench/run_all.sh first_job 400 3       # first_job, stress test size=400

set -e

JOB="${1:-first_job}"
SIZE="${2:-100}"
REPEATS="${3:-1}"

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
INPUT_PATH="hdfs://localhost:9000/flight/bench_${SIZE}/"
OUTPUT_BASE="hdfs://localhost:9000/flight/output"
METRICS_CSV="$PROJECT_DIR/metrics/$JOB/results.csv"
SNIPPETS_DIR="$PROJECT_DIR/report/$JOB/snippets"

mkdir -p "$PROJECT_DIR/metrics/$JOB" "$SNIPPETS_DIR"

# CSV header se il file non esiste ancora.
if [ ! -f "$METRICS_CSV" ]; then
    echo "framework,size,run,wall_seconds,exit_code,timestamp" > "$METRICS_CSV"
fi

# Verifica che INPUT_PATH esista su HDFS.
if ! hdfs dfs -test -d "$INPUT_PATH" 2>/dev/null; then
    echo "ERROR: input path non esiste su HDFS: $INPUT_PATH" >&2
    echo "       size disponibili: $(hdfs dfs -ls /flight 2>/dev/null | awk '{print $NF}' | grep -E '/flight/bench_[0-9]+$' | xargs -I{} basename {} | sed 's/bench_//' | tr '\n' ' ')" >&2
    echo "       (genera le size con: prep/prepare.sh && prep/replicate.sh)" >&2
    exit 1
fi

# Mappa "framework_id" -> "directory_name" (bash 3.2 no associative arrays).
job_dir() {
    case "$1" in
        mapreduce) echo "MapReduce" ;;
        sparkcore) echo "sparkCore" ;;
        sparksql)  echo "sparkSQL"  ;;
        hiveql)    echo "hiveQL"    ;;
        *) echo "" ;;
    esac
}

FRAMEWORKS=(mapreduce sparkcore sparksql hiveql)

run_one() {
    local fw="$1"
    local run_n="$2"
    local dir="$(job_dir "$fw")"
    local out="${OUTPUT_BASE}/${fw}_${SIZE}"
    local logfile="/tmp/${JOB}_${fw}_${SIZE}_run${run_n}.log"

    echo "----- ${fw} (job=${JOB}, size=${SIZE}, run=${run_n}) -----"

    hdfs dfs -rm -r -f "$out" "${out}_tmp_stage1" 2>/dev/null || true

    local start=$(date +%s)
    local exit_code=0
    INPUT_PATH="$INPUT_PATH" OUTPUT_PATH="$out" \
        "$PROJECT_DIR/${JOB}/${dir}/run.sh" > "$logfile" 2>&1 || exit_code=$?
    local end=$(date +%s)
    local wall=$((end - start))

    echo "  exit=${exit_code}  wall=${wall}s  log=${logfile}"
    echo "${fw},${SIZE},${run_n},${wall},${exit_code},$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$METRICS_CSV"

    # Snippet: prime 10 righe dell'output JSON. Solo per la prima run riuscita.
    # Glob "*" prende sia MR/Spark (part-*) che Hive (000000_*); grep filtra
    # via i marker _SUCCESS vuoti.
    if [ "$exit_code" = "0" ] && [ "$run_n" = "1" ]; then
        hdfs dfs -cat "${out}/*" 2>/dev/null | grep -v "^$" | head -10 \
            > "$SNIPPETS_DIR/${fw}_${SIZE}.json" || true
        local count=$(wc -l < "$SNIPPETS_DIR/${fw}_${SIZE}.json" 2>/dev/null | tr -d ' ')
        echo "  snippet -> $SNIPPETS_DIR/${fw}_${SIZE}.json (${count} righe)"
    fi
}

echo "=== ${JOB} bench: size=${SIZE}, repeats=${REPEATS} ==="
echo "INPUT  = ${INPUT_PATH}"
echo "OUTPUT = ${OUTPUT_BASE}/<framework>_${SIZE}/"
echo

for run_n in $(seq 1 "$REPEATS"); do
    for fw in "${FRAMEWORKS[@]}"; do
        run_one "$fw" "$run_n"
    done
done

echo
echo "=== Riepilogo (ultime $((4 * REPEATS)) righe di metrics/$JOB/results.csv) ==="
tail -$((4 * REPEATS)) "$METRICS_CSV" | column -t -s,
echo
echo "Snippet (prime 10 righe per framework):"
ls -l "$SNIPPETS_DIR"/*_"$SIZE".json 2>/dev/null
