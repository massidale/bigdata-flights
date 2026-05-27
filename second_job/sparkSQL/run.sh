#!/usr/bin/env bash
# Esegue il secondo job in Spark SQL (DataFrame / Catalyst).
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MASTER="${SPARK_MASTER:-local[*]}"

# Spark 3.5.8 supporta Python 3.7-3.11 (vedi note in sparkCore/run.sh).
export PYSPARK_PYTHON=/usr/local/bin/python3.11
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11

export INPUT_PATH="${INPUT_PATH:-hdfs://localhost:9000/flight/sample/}"

spark-submit --master "$MASTER" "$SCRIPT_DIR/second-job.py"
