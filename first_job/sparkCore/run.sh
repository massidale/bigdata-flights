#!/usr/bin/env bash
# Esegue il primo job in Spark Core (RDD API).
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Master Spark (override: SPARK_MASTER=yarn ./run.sh).
MASTER="${SPARK_MASTER:-local[*]}"

# Spark 3.5.8 supporta ufficialmente Python 3.7-3.11. Il python3 di default su
# questo Mac e' 3.14 (homebrew), che manda in stack overflow cloudpickle quando
# serializza le closure lambda/funzioni custom. Forzo 3.11.
export PYSPARK_PYTHON=/usr/local/bin/python3.11
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11

# INPUT_PATH parametrizzabile via env, letto dal driver Python tramite os.environ.
# Lo riesporto qui per chiarezza (spark-submit propaga le env del processo padre).
export INPUT_PATH="${INPUT_PATH:-hdfs://localhost:9000/flight/sample/}"

spark-submit --master "$MASTER" "$SCRIPT_DIR/first-job.py"
