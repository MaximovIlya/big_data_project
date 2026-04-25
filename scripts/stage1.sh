#!/bin/bash
# Stage I: Data Collection & Ingestion
# Load CSV → PostgreSQL, then Sqoop → HDFS

set -e

echo "=== Stage I: PostgreSQL + Sqoop ==="

PG_HOST="${PG_HOST:-hadoop-04.uni.innopolis.ru}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-team21_projectdb}"
PG_USER="${PG_USER:-team21}"
PG_PASSWORD="${PG_PASSWORD:-muYLyFnzeY4xZzcD}"
DATA_FILE="${DATA_FILE:-data/sf_incidents.csv}"
HDFS_BASE="${HDFS_BASE:-/user/$(whoami)/sf_incidents}"
HDFS_RAW="$HDFS_BASE/raw"

export PG_PASSWORD

# ── Steps 1-2: Create DB, load CSV into PostgreSQL ─────────────────────────
echo "--- Steps 1-2: PostgreSQL setup + CSV load ---"

python3 scripts/stage1_pg_load.py \
    --host      "$PG_HOST" \
    --port      "$PG_PORT" \
    --db        "$PG_DB" \
    --user      "$PG_USER" \
    --password  "$PG_PASSWORD" \
    --data-file "$DATA_FILE" \
    --sql-dir   sql

# ── Step 3: Sqoop Import → HDFS ────────────────────────────────────────────
echo "--- Step 3: Sqoop import to HDFS ---"

hdfs dfs -rm -r -f "$HDFS_RAW"

SQOOP_HOST="${SQOOP_PG_HOST:-${PG_HOST}}"
sqoop import \
    --connect "jdbc:postgresql://$SQOOP_HOST:$PG_PORT/$PG_DB" \
    --username "$PG_USER" \
    --password "$PG_PASSWORD" \
    --table sf_incidents \
    --target-dir "$HDFS_RAW" \
    --as-parquetfile \
    --compress \
    --compression-codec snappy \
    --num-mappers 4 \
    --fetch-size 10000 \
    --split-by row_id \
    --map-column-java latitude=Double,longitude=Double,incident_year=Integer

echo "Sqoop import complete. Data at: $HDFS_RAW"
hdfs dfs -ls "$HDFS_RAW"

echo "=== Stage I complete ==="
