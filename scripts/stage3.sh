#!/bin/bash
# Stage III: Data Analysis — Spark ML Classification Models
# Trains Random Forest, LinearSVC, and Naive Bayes on YARN cluster

set -e

echo "=== Stage III: Spark ML ==="

HIVE_DB="${HIVE_DB:-sf_incidents_db}"
SPARK_MASTER="${SPARK_MASTER:-yarn}"

spark-submit \
    --master "$SPARK_MASTER" \
    --deploy-mode client \
    --driver-memory 8g \
    --executor-memory 6g \
    --executor-cores 4 \
    --num-executors 6 \
    --conf "spark.sql.shuffle.partitions=200" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.driver.maxResultSize=4g" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    scripts/stage3_ml.py \
    --hive-db "$HIVE_DB" \
    --output-dir output/metrics \
    --models-dir models \
    --predictions-dir output/predictions \
    --top-n-categories 10 \
    --cv-folds 5

echo "=== Stage III complete. Models in models/, metrics in output/metrics/ ==="
