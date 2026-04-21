# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

End-to-end big data pipeline analyzing SF Police Department Incident Reports (2018–present). Goal: predict `incident_category` (multi-class classification) from time, location, and contextual features. Dataset: `data/sf_incidents.csv` (500k+ rows, download from Kaggle).

## Running the Pipeline

```bash
# Full pipeline (grader runs only this)
bash main.sh

# Individual stages
bash scripts/preprocess.sh   # env checks, create output dirs
bash scripts/stage1.sh       # PostgreSQL load + Sqoop → HDFS
bash scripts/stage2.sh       # Hive tables + PySpark EDA
bash scripts/stage3.sh       # Spark ML (RF, LinearSVC, NaiveBayes)
bash scripts/stage4.sh       # Streamlit dashboard
bash scripts/postprocess.sh  # cleanup/summary

# PySpark scripts directly (for dev/debug)
spark-submit --master yarn scripts/stage2_eda.py --hive-db sf_incidents_db --output-dir output/eda
spark-submit --master yarn scripts/stage3_ml.py --hive-db sf_incidents_db --output-dir output/metrics

# Lint all Python scripts (runs automatically in main.sh)
pylint scripts
```

## Environment Variables

All stages read from environment; `preprocess.sh` exports defaults:

| Variable | Default | Used by |
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

## Architecture & Data Flow

```
data/sf_incidents.csv
  → PostgreSQL (sf_incidents table, sql/create_tables.sql)
  → HDFS ($HDFS_BASE/raw/, via Sqoop as Parquet+Snappy)
  → Hive external table incidents_raw (sql/create_hive_tables.hql)
  → Hive managed table incidents_parquet (PARQUET + Snappy)
  → scripts/stage2_eda.py  → output/eda/*.csv  (8 EDA insights)
  → scripts/stage3_ml.py   → models/{random_forest,linear_svc,naive_bayes}/
                           → output/metrics/*.json
                           → output/predictions/*.csv
  → scripts/stage4_app.py  → Streamlit dashboard reading output/
```

**`main.sh` must not be modified** — it is used as-is by the grader.

## Key Implementation Notes

### Stage I (PostgreSQL + Sqoop)
- CSV is loaded via a `sf_incidents_staging` table (all TEXT) first, then cast/validated into `sf_incidents` with `ON CONFLICT DO NOTHING`. This avoids type errors on dirty rows.
- Sqoop imports as Parquet+Snappy with 4 mappers; type hints (`--map-column-java`) are needed for `latitude`, `longitude`, `incident_year`.

### Stage II (Hive + EDA)
- `incidents_raw` is an EXTERNAL Hive table — do not drop HDFS data before dropping this table.
- `incidents_parquet` filters out rows with NULL `incident_category`, `latitude`, or `longitude`.
- The HQL file receives `$HDFS_RAW` as `${hdfsloc}` via `--hivevar hdfsloc=...`.
- All 8 EDA results are saved via `df.coalesce(1).write.csv(...)` — one part file per insight directory.

### Stage III (Spark ML)
- All Python ML code uses **PySpark MLlib only** (no scikit-learn) — must run on YARN.
- Target: top-10 `incident_category` labels; remaining categories are bucketed as `"Other"`.
- Features: `hour`, `day_of_week_num`, `month`, `year`, `latitude`, `longitude`, `police_district` (StringIndexer → OneHotEncoder).
- Grading requires: RF (27 param combos), LinearSVC (27 combos), NaiveBayes; k-fold CV (5 folds); accuracy + weighted F1 reported per model.
- Models are saved with `model.write().overwrite().save("models/<name>")`.

### Stage IV (Streamlit)
- Reads CSVs from `output/eda/` and `output/metrics/` — does not connect to Spark/Hive directly.
- Run with: `streamlit run scripts/stage4_app.py`.

## Code Quality

`pylint scripts` runs at the end of `main.sh`. Python files in `scripts/` must pass pylint without disabling checks arbitrarily. Use `pylint: disable=...` inline only when unavoidable (e.g., PySpark dynamic attributes).

## Output Structure

```
output/
  eda/insight1_top_categories/   # one CSV part file per insight
  eda/insight2_by_hour/
  ...
  eda/summary_stats/
  metrics/                       # model JSON metrics
  predictions/                   # sample prediction CSVs
models/
  random_forest/                 # saved Spark ML PipelineModel
  linear_svc/
  naive_bayes/
```
