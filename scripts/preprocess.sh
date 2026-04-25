#!/bin/bash
# Stage 0: Pre-processing — environment checks and output directory setup

set -e

echo "=== Pre-processing: Environment Setup ==="

# ── Configuration ──────────────────────────────────────────────────────────
export PG_HOST="${PG_HOST:-hadoop-04.uni.innopolis.ru}"
export PG_PORT="${PG_PORT:-5432}"
export PG_DB="${PG_DB:-team21_projectdb}"
export PG_USER="${PG_USER:-team21}"
export PG_PASSWORD="${PG_PASSWORD:-muYLyFnzeY4xZzcD}"
export HDFS_BASE="${HDFS_BASE:-/user/$(whoami)/sf_incidents}"
export HIVE_DB="${HIVE_DB:-team21_projectdb}"
export DATA_FILE="${DATA_FILE:-data/sf_incidents.csv}"
export SPARK_MASTER="${SPARK_MASTER:-yarn}"
export SPARK_DEPLOY_MODE="${SPARK_DEPLOY_MODE:-client}"

# ── Validate data file exists ───────────────────────────────────────────────
if [ ! -f "$DATA_FILE" ]; then
    echo "ERROR: Dataset file not found at '$DATA_FILE'."
    echo "Please download it from Kaggle and place it at $DATA_FILE"
    exit 1
fi

ROW_COUNT=$(wc -l < "$DATA_FILE")
echo "Dataset found: $DATA_FILE ($ROW_COUNT lines)"

# ── Create output directories ───────────────────────────────────────────────
mkdir -p output/eda
mkdir -p output/metrics
mkdir -p output/predictions
mkdir -p output/superset_export
mkdir -p models/random_forest
mkdir -p models/linear_svc
mkdir -p models/naive_bayes

echo "Output directories created."

# ── Check required commands ─────────────────────────────────────────────────
for cmd in psql sqoop hive spark-submit hdfs; do
    if command -v "$cmd" &>/dev/null; then
        echo "  [OK] $cmd found: $(command -v $cmd)"
    else
        echo "  [WARN] $cmd not found in PATH — stage using it may fail"
    fi
done

# ── Check Python packages ───────────────────────────────────────────────────
python3 -c "import pyspark; print('  [OK] pyspark', pyspark.__version__)"
python3 -c "import pandas; print('  [OK] pandas', pandas.__version__)"
python3 -c "import streamlit; print('  [OK] streamlit', streamlit.__version__)"

echo "=== Pre-processing complete ==="
