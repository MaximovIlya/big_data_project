#!/bin/bash
# Stage II: Data Storage & Preparation
# Create Hive tables + run PySpark EDA

set -e

echo "=== Stage II: Hive + Spark SQL EDA ==="

HDFS_BASE="${HDFS_BASE:-/user/$(whoami)/sf_incidents}"
HDFS_RAW="$HDFS_BASE/raw"
HIVE_DB="${HIVE_DB:-sf_incidents_db}"
SPARK_MASTER="${SPARK_MASTER:-yarn}"

# ── Step 1: Create Hive tables ─────────────────────────────────────────────
echo "--- Step 1: Creating Hive tables ---"

hive --hivevar hdfsloc="$HDFS_RAW" -f sql/create_hive_tables.hql

echo "Hive tables created in database '$HIVE_DB'."

# Verify table creation
hive -e "USE $HIVE_DB; SHOW TABLES;"

# ── Step 2: Run PySpark EDA ────────────────────────────────────────────────
echo "--- Step 2: Running PySpark EDA ---"

spark-submit \
    --master "$SPARK_MASTER" \
    --deploy-mode client \
    --driver-memory 4g \
    --executor-memory 4g \
    --executor-cores 2 \
    --num-executors 4 \
    --conf "spark.sql.shuffle.partitions=200" \
    --conf "spark.sql.adaptive.enabled=true" \
    scripts/stage2_eda.py \
    --hive-db "$HIVE_DB" \
    --output-dir output/eda

echo "=== Stage II complete. EDA results in output/eda/ ==="
