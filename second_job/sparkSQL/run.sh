#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

export PYSPARK_PYTHON=/usr/local/bin/python3.11
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11

export INPUT_PATH="${INPUT_PATH:-hdfs://localhost:9000/flight/sample/}"

    spark-submit --master yarn --deploy-mode client \
        $SCRIPT_DIR/esercitazione_3_2_spark_sql.py $INPUT_PATH $S3_BUCKET/output/sparkcore/$SIZE/