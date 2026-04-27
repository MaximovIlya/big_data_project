#!/bin/bash
# Stage II: Data Storage & Preparation
# Create Hive tables + run PySpark EDA

set -e

echo "=== Stage II: Hive + Spark SQL EDA ==="

HDFS_BASE="${HDFS_BASE:-/user/$(whoami)/sf_incidents}"
HDFS_RAW="$HDFS_BASE/raw"
HIVE_DB="${HIVE_DB:-sf_incidents_db}"
SPARK_MASTER="${SPARK_MASTER:-yarn}"

# ── Step 1: Run PySpark EDA (creates Hive tables + runs analysis) ──────────
echo "--- Step 1: Running PySpark EDA (creates Hive tables + EDA) ---"

spark-submit \
    --master "$SPARK_MASTER" \
    --deploy-mode client \
    --driver-memory 4g \
    --executor-memory 4g \
    --executor-cores 2 \
    --num-executors 4 \
    --conf "spark.sql.shuffle.partitions=200" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.warehouse.dir=hdfs:///user/$(whoami)/spark-warehouse" \
    scripts/stage2_eda.py \
    --hive-db "$HIVE_DB" \
    --hdfs-raw "$HDFS_RAW" \
    --output-dir output/eda

# Copy EDA output from HDFS to local FS so Stage IV can read it
rm -rf output/eda
hdfs dfs -get output/eda output/eda

echo "=== Stage II complete. EDA results in output/eda/ ==="
