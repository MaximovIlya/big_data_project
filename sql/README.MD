# sql/

SQL and HiveQL files defining the database schema for Stages I and II.

## Files

| File | Used By | Purpose |
|---|---|---|
| `create_tables.sql` | `stage1_pg_load.py` | PostgreSQL DDL — staging and main tables |
| `create_hive_tables.hql` | `stage2_eda.py` (via Spark SQL) | Hive DDL — external and managed tables |

## create_tables.sql

Defines two PostgreSQL tables:

### sf_incidents_staging
All 35 columns as `TEXT` — used to load the raw CSV without type errors. The `COPY` command never fails on a text table regardless of dirty values.

### sf_incidents
The typed, constrained production table. Key constraints:

```sql
row_id              BIGINT PRIMARY KEY
incident_datetime   TIMESTAMP NOT NULL
incident_year       SMALLINT NOT NULL CHECK (incident_year >= 2018)
latitude            DOUBLE PRECISION CHECK (latitude BETWEEN 37.0 AND 38.5)
longitude           DOUBLE PRECISION CHECK (longitude BETWEEN -123.5 AND -122.0)
```

Five indexes are created for query performance:
- `idx_sf_incidents_category` on `incident_category`
- `idx_sf_incidents_datetime` on `incident_datetime`
- `idx_sf_incidents_district` on `police_district`
- `idx_sf_incidents_year` on `incident_year`
- `idx_sf_incidents_geo` on `(latitude, longitude)`

## create_hive_tables.hql

Defines the Hive database and tables used by PySpark EDA and ML scripts.

### Database
```sql
CREATE DATABASE sf_incidents_db
LOCATION 'hdfs:///user/team21/spark-warehouse/sf_incidents_db.db';
```

### incidents_raw (EXTERNAL)
Points to the Sqoop HDFS output directory. Dropping this table does **not** delete the HDFS data.

Column types match Sqoop 1.4.7 Parquet output on this cluster:
- `incident_datetime` → `BIGINT` (INT64 milliseconds since epoch)
- `incident_date` → `BIGINT` (INT64 milliseconds since epoch)
- `incident_time` → `BIGINT` (INT64 milliseconds since midnight)
- `filed_online` → `BOOLEAN`
- All text columns → `STRING`
- Numeric columns → `INT` or `DOUBLE`

### incidents_parquet (MANAGED)
Created as `CREATE TABLE ... AS SELECT` from `incidents_raw` with:
- NULL filtering on `incident_category`, `latitude`, `longitude`
- Time conversions: `from_unixtime(incident_datetime/1000)`, hour extraction
- Stored as PARQUET with Snappy compression

### v_incident_summary (VIEW)
Aggregates incident counts by category — used for quick summary queries.

## Sqoop Type Mapping Notes

This cluster (Sqoop 1.4.7 Arenadata) maps PostgreSQL types to Parquet as:

| PostgreSQL | Parquet / Java | Hive DDL |
|---|---|---|
| TIMESTAMP | INT64 (ms epoch) | BIGINT |
| DATE | INT64 (ms epoch) | BIGINT |
| TIME | INT64 (ms midnight) | BIGINT |
| BOOLEAN | BOOLEAN | BOOLEAN |
| VARCHAR/TEXT | BINARY | STRING |
| INTEGER/SMALLINT | INT32 | INT |
| BIGINT | INT64 | BIGINT |
| DOUBLE PRECISION | DOUBLE | DOUBLE |

The Hive DDL must use these types exactly — mismatches cause `SchemaColumnConvertNotSupportedException` in the Spark vectorized Parquet reader.
