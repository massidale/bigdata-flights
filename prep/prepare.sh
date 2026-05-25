#!/usr/bin/env bash
# Lancia prep/prepare.py: produce /flight/clean/ dal CSV originale.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MASTER="${SPARK_MASTER:-local[*]}"

# Spark 3.5.8 richiede Python 3.7-3.11. Forzo 3.11 (3.14 di default rompe cloudpickle).
export PYSPARK_PYTHON=/usr/local/bin/python3.11
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11

export INPUT_PATH="${INPUT_PATH:-hdfs://localhost:9000/flight/flight_data_2024.csv}"
export OUTPUT_PATH="${OUTPUT_PATH:-hdfs://localhost:9000/flight/clean}"

spark-submit --master "$MASTER" "$SCRIPT_DIR/prepare.py"
