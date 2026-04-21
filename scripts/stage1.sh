#!/bin/bash
# Stage I: Data Collection & Ingestion
# Load CSV → PostgreSQL, then Sqoop → HDFS

set -e

echo "=== Stage I: PostgreSQL + Sqoop ==="

PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-sf_incidents}"
PG_USER="${PG_USER:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-postgres}"
DATA_FILE="${DATA_FILE:-data/sf_incidents.csv}"
HDFS_BASE="${HDFS_BASE:-/user/$(whoami)/sf_incidents}"
HDFS_RAW="$HDFS_BASE/raw"

export PGPASSWORD="$PG_PASSWORD"

# ── Step 1: Create PostgreSQL database and table ───────────────────────────
echo "--- Step 1: Setting up PostgreSQL ---"

psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
    -c "CREATE DATABASE $PG_DB;" 2>/dev/null \
    || echo "Database '$PG_DB' already exists, continuing."

psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
    -f sql/create_tables.sql

echo "PostgreSQL schema created."

# ── Step 2: Load CSV via staging table ────────────────────────────────────
echo "--- Step 2: Loading CSV into PostgreSQL ---"

# Staging table mirrors all 35 CSV columns (all TEXT — no type errors on load)
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" <<'SQL'
DROP TABLE IF EXISTS sf_incidents_staging;

CREATE TABLE sf_incidents_staging (
    incident_datetime        TEXT,
    incident_date            TEXT,
    incident_time            TEXT,
    incident_year            TEXT,
    incident_day_of_week     TEXT,
    report_datetime          TEXT,
    row_id                   TEXT,
    incident_id              TEXT,
    incident_number          TEXT,
    cad_number               TEXT,
    report_type_code         TEXT,
    report_type_description  TEXT,
    filed_online             TEXT,
    incident_code            TEXT,
    incident_category        TEXT,
    incident_subcategory     TEXT,
    incident_description     TEXT,
    resolution               TEXT,
    intersection             TEXT,
    cnn                      TEXT,
    police_district          TEXT,
    analysis_neighborhood    TEXT,
    supervisor_district      TEXT,
    supervisor_district_2012 TEXT,
    latitude                 TEXT,
    longitude                TEXT,
    point                    TEXT,
    neighborhoods            TEXT,
    esncag_boundary          TEXT,
    central_market_boundary  TEXT,
    civic_center_boundary    TEXT,
    hsoc_zones               TEXT,
    iin_areas                TEXT,
    current_supervisor_dist  TEXT,
    current_police_dist      TEXT
);
SQL

# COPY with HEADER skips the first row; columns mapped positionally
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
    -c "\COPY sf_incidents_staging FROM '$DATA_FILE' WITH (FORMAT CSV, HEADER, ENCODING 'UTF8');"

echo "CSV loaded into staging table."

# Insert into main table with type casting
# Datetime format in dataset: '2023/03/13 11:41:00 PM'
psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" <<'SQL'
INSERT INTO sf_incidents (
    row_id, incident_datetime, incident_date, incident_time,
    incident_year, incident_day_of_week, report_datetime,
    incident_id, incident_number, cad_number,
    report_type_code, report_type_description, filed_online,
    incident_code, incident_category, incident_subcategory,
    incident_description, resolution, intersection, cnn,
    police_district, analysis_neighborhood,
    supervisor_district, supervisor_district_2012,
    latitude, longitude, point
)
SELECT
    NULLIF(row_id, '')::BIGINT,
    TO_TIMESTAMP(NULLIF(incident_datetime, ''), 'YYYY/MM/DD HH:MI:SS AM'),
    TO_DATE(NULLIF(incident_date, ''), 'YYYY/MM/DD'),
    NULLIF(incident_time, '')::TIME,
    NULLIF(incident_year, '')::SMALLINT,
    NULLIF(incident_day_of_week, ''),
    TO_TIMESTAMP(NULLIF(report_datetime, ''), 'YYYY/MM/DD HH:MI:SS AM'),
    NULLIF(incident_id, '')::BIGINT,
    NULLIF(incident_number, '')::BIGINT,
    NULLIF(cad_number, '')::BIGINT,
    NULLIF(report_type_code, ''),
    NULLIF(report_type_description, ''),
    CASE WHEN lower(filed_online) = 'true' THEN TRUE ELSE FALSE END,
    NULLIF(incident_code, '')::INTEGER,
    NULLIF(incident_category, ''),
    NULLIF(incident_subcategory, ''),
    NULLIF(incident_description, ''),
    NULLIF(resolution, ''),
    NULLIF(intersection, ''),
    NULLIF(cnn, '')::BIGINT,
    NULLIF(police_district, ''),
    NULLIF(analysis_neighborhood, ''),
    NULLIF(supervisor_district, '')::SMALLINT,
    NULLIF(supervisor_district_2012, '')::SMALLINT,
    NULLIF(latitude, '')::DOUBLE PRECISION,
    NULLIF(longitude, '')::DOUBLE PRECISION,
    NULLIF(point, '')
FROM sf_incidents_staging
WHERE NULLIF(row_id, '') IS NOT NULL
  AND NULLIF(incident_category, '') IS NOT NULL
  AND NULLIF(incident_year, '')::SMALLINT >= 2018
ON CONFLICT (row_id) DO NOTHING;

DROP TABLE sf_incidents_staging;
SQL

ROW_COUNT=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
    -t -c "SELECT COUNT(*) FROM sf_incidents;")
echo "Rows loaded into sf_incidents:$ROW_COUNT"

# ── Step 3: Sqoop Import → HDFS ────────────────────────────────────────────
echo "--- Step 3: Sqoop import to HDFS ---"

hdfs dfs -rm -r -f "$HDFS_RAW"

sqoop import \
    --connect "jdbc:postgresql://$PG_HOST:$PG_PORT/$PG_DB" \
    --username "$PG_USER" \
    --password "$PG_PASSWORD" \
    --table sf_incidents \
    --target-dir "$HDFS_RAW" \
    --as-parquetfile \
    --compress \
    --compression-codec snappy \
    --num-mappers 4 \
    --fetch-size 10000 \
    --map-column-java latitude=Double,longitude=Double,incident_year=Integer

echo "Sqoop import complete. Data at: $HDFS_RAW"
hdfs dfs -ls "$HDFS_RAW"

echo "=== Stage I complete ==="
