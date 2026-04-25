"""Stage I helper: load SF incidents CSV into PostgreSQL via psycopg2."""

import argparse
import os
import sys

import psycopg2
from psycopg2 import sql as pgsql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

_STAGING_DDL = """
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
"""

_FUNCTIONS_DDL = """
CREATE OR REPLACE FUNCTION safe_ts(val TEXT, fmt TEXT) RETURNS TIMESTAMP AS $$
BEGIN
  IF val IS NULL OR val = '' THEN RETURN NULL; END IF;
  RETURN TO_TIMESTAMP(val, fmt);
EXCEPTION WHEN others THEN RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION safe_bigint(val TEXT) RETURNS BIGINT AS $$
BEGIN
  IF val IS NULL OR val = '' THEN RETURN NULL; END IF;
  RETURN val::NUMERIC::BIGINT;
EXCEPTION WHEN others THEN RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""

_INSERT_SQL = """
SET lc_time = 'C';

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
    safe_bigint(row_id),
    safe_ts(NULLIF(incident_datetime, ''), 'YYYY/MM/DD HH:MI:SS AM'),
    TO_DATE(NULLIF(incident_date, ''), 'YYYY/MM/DD'),
    NULLIF(incident_time, '')::TIME,
    NULLIF(incident_year, '')::SMALLINT,
    NULLIF(incident_day_of_week, ''),
    safe_ts(NULLIF(report_datetime, ''), 'YYYY/MM/DD HH:MI:SS AM'),
    safe_bigint(incident_id),
    safe_bigint(incident_number),
    safe_bigint(cad_number),
    NULLIF(report_type_code, ''),
    NULLIF(report_type_description, ''),
    CASE WHEN lower(filed_online) = 'true' THEN TRUE ELSE FALSE END,
    NULLIF(incident_code, '')::INTEGER,
    NULLIF(incident_category, ''),
    NULLIF(incident_subcategory, ''),
    NULLIF(incident_description, ''),
    NULLIF(resolution, ''),
    NULLIF(intersection, ''),
    safe_bigint(cnn),
    NULLIF(police_district, ''),
    NULLIF(analysis_neighborhood, ''),
    NULLIF(supervisor_district, '')::SMALLINT,
    NULLIF(supervisor_district_2012, '')::SMALLINT,
    NULLIF(latitude, '')::DOUBLE PRECISION,
    NULLIF(longitude, '')::DOUBLE PRECISION,
    NULLIF(point, '')
FROM sf_incidents_staging
WHERE safe_bigint(row_id) IS NOT NULL
  AND NULLIF(incident_category, '') IS NOT NULL
  AND NULLIF(incident_year, '')::SMALLINT >= 2018
ON CONFLICT (row_id) DO NOTHING;

DROP FUNCTION IF EXISTS safe_ts(TEXT, TEXT);
DROP FUNCTION IF EXISTS safe_bigint(TEXT);
DROP TABLE IF EXISTS sf_incidents_staging;
"""


def connect(host, port, user, password, dbname):
    """Return a psycopg2 connection."""
    return psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=dbname
    )


def ensure_database(host, port, user, password, dbname):
    """Create the target database if it does not exist.

    On shared clusters the user may only have access to a pre-created database
    (e.g. team21_projectdb) and cannot connect to the 'postgres' system DB.
    In that case we skip creation and verify access by connecting directly.
    """
    try:
        conn = connect(host, port, user, password, "postgres")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", [dbname])
            if cur.fetchone():
                print(f"Database '{dbname}' already exists, continuing.")
            else:
                cur.execute(pgsql.SQL("CREATE DATABASE {}").format(pgsql.Identifier(dbname)))
                print(f"Database '{dbname}' created.")
        conn.close()
    except psycopg2.OperationalError:
        # No access to postgres system DB — database pre-created by admin, proceed directly.
        print(f"Cannot connect to 'postgres' system DB; assuming '{dbname}' already exists.")


def run_sql_file(conn, path):
    """Execute a SQL file."""
    with open(path, "r", encoding="utf-8") as file_handle:
        ddl = file_handle.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def load_csv(conn, data_file):
    """Stream CSV into staging table via COPY."""
    abs_path = os.path.abspath(data_file)
    copy_sql = (
        "COPY sf_incidents_staging FROM STDIN "
        "WITH (FORMAT CSV, HEADER, ENCODING 'UTF8')"
    )
    with open(abs_path, "r", encoding="utf-8") as file_handle:
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, file_handle)
    conn.commit()
    print("CSV loaded into staging table.")


def insert_and_cast(conn):
    """Create helper functions, insert with type casting, then clean up."""
    with conn.cursor() as cur:
        cur.execute(_FUNCTIONS_DDL)
        cur.execute(_INSERT_SQL)
    conn.commit()


def row_count(conn):
    """Return the number of rows in sf_incidents."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sf_incidents")
        return cur.fetchone()[0]


def main():
    """Parse args and run the full PostgreSQL load sequence."""
    parser = argparse.ArgumentParser(description="Load SF incidents CSV into PostgreSQL")
    parser.add_argument("--host",     default=os.environ.get("PG_HOST", "localhost"))
    parser.add_argument("--port",     default=int(os.environ.get("PG_PORT", "5432")), type=int)
    parser.add_argument("--db",       default=os.environ.get("PG_DB",   "sf_incidents"))
    parser.add_argument("--user",     default=os.environ.get("PG_USER", "postgres"))
    parser.add_argument("--password", default=os.environ.get("PG_PASSWORD", "postgres"))
    parser.add_argument("--data-file",
                        default=os.environ.get("DATA_FILE", "data/sf_incidents.csv"))
    parser.add_argument("--sql-dir",  default="sql")
    args = parser.parse_args()

    ensure_database(args.host, args.port, args.user, args.password, args.db)

    conn = connect(args.host, args.port, args.user, args.password, args.db)
    try:
        ddl_path = os.path.join(args.sql_dir, "create_tables.sql")
        print("--- Creating schema ---")
        run_sql_file(conn, ddl_path)
        print("Schema created.")

        print("--- Creating staging table ---")
        with conn.cursor() as cur:
            cur.execute(_STAGING_DDL)
        conn.commit()

        print(f"--- Loading CSV: {args.data_file} ---")
        load_csv(conn, args.data_file)

        print("--- Inserting with type casting ---")
        insert_and_cast(conn)

        count = row_count(conn)
        print(f"Rows loaded into sf_incidents: {count}")
    finally:
        conn.close()

    sys.exit(0)


if __name__ == "__main__":
    main()
