#!/usr/bin/env bash
# Lancia prep/replicate.py: genera bench_50/100/200/400 da /flight/clean/.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MASTER="${SPARK_MASTER:-local[*]}"

export PYSPARK_PYTHON=/usr/local/bin/python3.11
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11

export INPUT_PATH="${INPUT_PATH:-hdfs://localhost:9000/flight/clean}"
export OUTPUT_BASE="${OUTPUT_BASE:-hdfs://localhost:9000/flight}"

spark-submit --master "$MASTER" "$SCRIPT_DIR/replicate.py"
