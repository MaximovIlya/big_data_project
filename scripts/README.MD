# scripts/

All pipeline execution scripts. The grader runs `bash main.sh` from the project root, which calls these scripts in sequence.

## Script Overview

| Script | Stage | Description |
|---|---|---|
| `preprocess.sh` | 0 | Environment checks, output directory setup |
| `stage1.sh` | I | PostgreSQL load + Sqoop → HDFS |
| `stage1_pg_load.py` | I | Python helper: PostgreSQL staging + typed insert |
| `stage2.sh` | II | Hive table creation + PySpark EDA + HDFS→local copy |
| `stage2_eda.py` | II | PySpark: creates Hive tables and computes 8 EDA insights |
| `stage3.sh` | III | Spark MLlib training on YARN |
| `stage3_ml.py` | III | PySpark: feature engineering + 3 models + hyperparameter tuning |
| `stage4.sh` | IV | Superset export + Streamlit launch |
| `stage4_superset.py` | IV | Generates Superset v1 export ZIP from EDA CSVs |
| `stage4_app.py` | IV | Streamlit 5-page interactive dashboard |
| `postprocess.sh` | post | Output summary, model verification, metrics display |

## Running Individual Stages

```bash
bash scripts/preprocess.sh    # Check tools, create output dirs
bash scripts/stage1.sh        # PostgreSQL + Sqoop (requires PG + HDFS)
bash scripts/stage2.sh        # Hive + EDA (requires YARN + Hive metastore)
bash scripts/stage3.sh        # ML training (requires YARN, ~1-2 hours)
bash scripts/stage4.sh        # Dashboard (Superset export + Streamlit)
bash scripts/postprocess.sh   # Summary verification
```

## Environment Variables

All scripts read these environment variables (with defaults from `preprocess.sh`):

| Variable | Default | Used By |
|---|---|---|
| `PG_HOST` | `localhost` | stage1 |
| `PG_PORT` | `5432` | stage1 |
| `PG_DB` | `sf_incidents` | stage1 |
| `PG_USER` | `postgres` | stage1 |
| `PG_PASSWORD` | `postgres` | stage1 |
| `HDFS_BASE` | `/user/$USER/sf_incidents` | stage1, stage2 |
| `HIVE_DB` | `sf_incidents_db` | stage2, stage3 |
| `SPARK_MASTER` | `yarn` | stage2, stage3 |
| `DATA_FILE` | `data/sf_incidents.csv` | preprocess, stage1 |

## Stage Details

### preprocess.sh
- Verifies `data/sf_incidents.csv` exists
- Creates `output/eda/`, `output/metrics/`, `output/predictions/`, `output/superset_export/`
- Creates `models/random_forest/`, `models/linear_svc/`, `models/naive_bayes/`
- Checks for required tools: `sqoop`, `spark-submit`, `hdfs`, `pyspark`, `pandas`
- Streamlit check is non-fatal (WARN only) — pipeline continues if not installed

### stage1.sh + stage1_pg_load.py
1. Calls `stage1_pg_load.py` to:
   - Create `sf_incidents_staging` (all TEXT columns) and `sf_incidents` (typed)
   - `COPY` CSV into staging table
   - Cast and insert into typed table with `ON CONFLICT DO NOTHING`
2. Runs Sqoop import: PostgreSQL → HDFS as Parquet+Snappy, 4 mappers
3. Verifies HDFS output with `hdfs dfs -ls`

### stage2.sh + stage2_eda.py
1. Submits `stage2_eda.py` via `spark-submit --master yarn`
2. `stage2_eda.py`:
   - Drops and recreates `sf_incidents_db` (resets Hive metastore warehouse location)
   - Creates external table `incidents_raw` over Sqoop HDFS output
   - Creates managed table `incidents_parquet` (Parquet+Snappy, NULLs filtered)
   - Runs 8 EDA queries and saves results as CSV to HDFS `output/eda/`
3. Copies `output/eda/` from HDFS to local disk via `hdfs dfs -get`

### stage3.sh + stage3_ml.py
1. Submits `stage3_ml.py` via `spark-submit --master yarn` (8GB driver, 6GB executor × 6)
2. `stage3_ml.py`:
   - Loads `incidents_parquet` from Hive
   - Builds feature pipeline: hour, day_of_week, month, year, lat, lon, district
   - Trains RF (27-grid, 5-fold CV), LinearSVC/OvR (27-grid, 5-fold CV), NB (5-grid, 5-fold CV)
   - Saves models to `models/`, metrics to `output/metrics/`, predictions to `output/predictions/`

### stage4.sh + stage4_superset.py + stage4_app.py
1. Calls `stage4_superset.py`:
   - Loads EDA CSVs from `output/eda/` into SQLite `output/sf_incidents_eda.db`
   - Generates Superset v1 export ZIP at `output/superset_export/sf_incidents_dashboard.zip`
2. Launches Streamlit in background (port 8501) — non-fatal if not installed

## Code Quality

All Python scripts pass `pylint scripts/` at ≥9.81/10 as verified by `main.sh`.
