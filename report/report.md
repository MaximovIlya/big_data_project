# Big Data Pipeline: SF Police Incident Reports
## Final Project Report

**Course:** Big Data Systems  
**Dataset:** San Francisco Police Department Incident Reports 2018–Present  
**Source:** [Kaggle](https://www.kaggle.com/datasets/vivovinco/san-francisco-incident-reports-2018present)

---

## 1. Introduction

This project implements an end-to-end big data pipeline to analyze the San Francisco Police Department (SFPD) Incident Reports dataset. The dataset contains over 800,000 records of police incidents reported in San Francisco from 2018 to the present, encompassing 35 attributes including incident type, datetime, location, police district, and resolution status.

**Problem Statement:** Given an incident's temporal and spatial features (time of day, day of week, month, year, geographic coordinates, and police district), can we predict the incident category? This is a multi-class classification problem with 11 target classes (top 10 categories + "Other").

**Goals:**
1. Ingest and store the dataset in a distributed data stack (PostgreSQL → HDFS → Hive)
2. Conduct exploratory data analysis using Spark SQL to extract 8 actionable insights
3. Train and evaluate three machine learning classifiers using PySpark MLlib with hyperparameter tuning
4. Present findings through an interactive Streamlit dashboard

---

## 2. Dataset Description

### 2.1 Overview

| Property | Value |
|---|---|
| Source | San Francisco Open Data / Kaggle |
| Records | 796,456 rows |
| Columns | 35 |
| File Format | CSV (412.9 MB in memory) |
| Time Range | 2018-01-01 – 2023-11-24 |
| Update Frequency | Daily |

### 2.2 Key Columns

| Column | Type | Description |
|---|---|---|
| `Row ID` | INTEGER | Unique incident identifier |
| `Incident Datetime` | TIMESTAMP | Date and time the incident occurred |
| `Incident Year` | SMALLINT | Year of incident (2018+) |
| `Incident Day of Week` | TEXT | Day name (Monday–Sunday) |
| `Incident Category` | TEXT | **Target variable** — high-level crime type |
| `Incident Subcategory` | TEXT | More specific crime subcategory |
| `Police District` | TEXT | SFPD patrol district |
| `Analysis Neighborhood` | TEXT | Neighborhood name |
| `Latitude` | FLOAT | Geographic latitude (37.0–38.5) |
| `Longitude` | FLOAT | Geographic longitude (−123.5 to −122.0) |
| `Resolution` | TEXT | Outcome of the incident report |

### 2.3 Target Variable Distribution

The `Incident Category` field contains 60+ distinct values. For classification purposes, we retained the top 10 most frequent categories and grouped all remaining categories into an "Other" class, resulting in 11 classes total.

| Rank | Category | Count |
|---|---|---|
| 1 | Larceny Theft | 242,034 |
| 2 | Other Miscellaneous | 54,225 |
| 3 | Malicious Mischief | 53,860 |
| 4 | Assault | 48,501 |
| 5 | Non-Criminal | 46,987 |
| 6 | Burglary | 44,390 |
| 7 | Motor Vehicle Theft | 42,555 |
| 8 | Recovered Vehicle | 31,927 |
| 9 | Fraud | 25,926 |
| 10 | Lost Property | 23,194 |
| — | Other (39 remaining categories) | 182,857 |

### 2.4 Data Quality

| Issue | Column(s) | Handling |
|---|---|---|
| Missing coordinates | `Latitude`, `Longitude` | Rows excluded from ML (not from EDA) |
| Missing police district | `Police District` | Replaced with `"Unknown"` |
| Missing category | `Incident Category` | Rows excluded from Hive ML table |
| Type parsing errors | `Incident Datetime` | `TO_TIMESTAMP(..., 'YYYY/MM/DD HH:MI:SS AM')` |
| Dirty rows (text in numeric cols) | Multiple | Loaded via staging table (all TEXT), cast on insert |

---

## 3. Architecture

```
data/sf_incidents.csv  (800K rows, 35 columns, ~500 MB)
       │
       ▼  STAGE I
┌─────────────────────────┐
│  PostgreSQL              │  ← staging table (all TEXT) → typed insert
│  sf_incidents DB         │    with constraints, indexes
└──────────┬──────────────┘
           │  Apache Sqoop (JDBC → HDFS, MapReduce, 4 mappers)
           ▼  STAGE I
┌─────────────────────────┐
│  HDFS                    │  ← Parquet + Snappy compression
│  /user/$USER/sf_incidents│
└──────────┬──────────────┘
           │  Hive External Table
           ▼  STAGE II
┌─────────────────────────┐
│  Apache Hive             │  ← incidents_raw (EXTERNAL)
│  sf_incidents_db         │    incidents_parquet (MANAGED, Parquet+Snappy)
└──────────┬──────────────┘
           │  Spark SQL (on YARN)
           ▼  STAGE II
┌─────────────────────────┐
│  8 EDA Insights          │  ← output/eda/ (CSV part files)
│  + Summary Stats         │
└──────────┬──────────────┘
           │  PySpark MLlib (on YARN)
           ▼  STAGE III
┌─────────────────────────┐
│  3 ML Models             │  ← RandomForest, LinearSVC, NaiveBayes
│  + CrossValidator        │    models/ (saved PipelineModels)
│  + Hyperparameter Grids  │    output/metrics/metrics_summary.json
└──────────┬──────────────┘
           │
           ▼  STAGE IV
┌─────────────────────────┐
│  Streamlit Dashboard     │  ← reads output/ CSVs (no Spark at runtime)
│  5 pages                 │    Data Overview, EDA, Models, Predictions, Map
└─────────────────────────┘
```

**Key design decisions:**
- **Staging table pattern** in PostgreSQL avoids type errors on dirty rows: all 35 columns loaded as TEXT, then cast and validated on INSERT with `ON CONFLICT DO NOTHING`.
- **Sqoop with `--map-column-java`** ensures correct Java types for `latitude`, `longitude`, `incident_year` during HDFS import.
- **External Hive table** (`incidents_raw`) preserves HDFS data on table drop; the managed `incidents_parquet` table filters nulls for ML readiness.
- **Streamlit reads CSVs only** — no Spark/Hive connection at runtime, enabling deployment on any machine.

---

## 4. Stage I — Data Ingestion

### 4.1 PostgreSQL Schema

The `sf_incidents` table is created with the following constraints:

```sql
CREATE TABLE sf_incidents (
    row_id              BIGINT PRIMARY KEY,
    incident_datetime   TIMESTAMP NOT NULL,
    incident_year       SMALLINT NOT NULL CHECK (incident_year >= 2018),
    latitude            DOUBLE PRECISION CHECK (latitude BETWEEN 37.0 AND 38.5),
    longitude           DOUBLE PRECISION CHECK (longitude BETWEEN -123.5 AND -122.0),
    incident_category   TEXT,
    police_district     TEXT,
    -- ... 28 additional columns
);
CREATE INDEX idx_sf_incidents_category ON sf_incidents (incident_category);
CREATE INDEX idx_sf_incidents_datetime ON sf_incidents (incident_datetime);
CREATE INDEX idx_sf_incidents_district ON sf_incidents (police_district);
```

**Loading strategy:**
1. Create `sf_incidents_staging` with all 35 columns as TEXT
2. `\COPY sf_incidents_staging FROM ... WITH (FORMAT CSV, HEADER)`
3. `INSERT INTO sf_incidents SELECT ... (with casts) FROM sf_incidents_staging ON CONFLICT DO NOTHING`

This approach handles malformed rows gracefully — invalid casts on individual rows do not abort the entire load.

### 4.2 Sqoop Import

```bash
sqoop import \
  --connect jdbc:postgresql://$PG_HOST:$PG_PORT/$PG_DB \
  --username $PG_USER \
  --password $PG_PASSWORD \
  --table sf_incidents \
  --target-dir $HDFS_BASE/raw \
  --as-parquetfile \
  --compress \
  --compression-codec snappy \
  --num-mappers 4 \
  --map-column-java latitude=Double,longitude=Double,incident_year=Integer
```

Result: HDFS directory `/user/$USER/sf_incidents/raw/` containing Parquet part files with Snappy compression.

---

## 5. Stage II — Storage & EDA

### 5.1 Hive Tables

Two tables are created in the `sf_incidents_db` database:

| Table | Type | Format | Purpose |
|---|---|---|---|
| `incidents_raw` | EXTERNAL | Parquet (from Sqoop) | Raw data, preserves HDFS on drop |
| `incidents_parquet` | MANAGED | Parquet + Snappy | Clean data for EDA and ML |

`incidents_parquet` is created with `CREATE TABLE ... AS SELECT` filtering out rows with NULL `incident_category`, `latitude`, or `longitude`.

### 5.2 EDA Insights

All 8 insights are computed using PySpark SQL on YARN and saved to `output/eda/` as CSV files (one directory per insight, containing a single Spark part file).

#### Insight 1 — Top 10 Incident Categories
Identifies the most frequently reported crime types. **Larceny Theft** is the dominant category, accounting for approximately 25% of all incidents.

#### Insight 2 — Incidents by Hour of Day
Reveals a bimodal distribution with peaks at noon and 6 PM, and a minimum between 3–6 AM. Suggests that most incidents are reported during daytime business hours.

#### Insight 3 — Incidents by Day of Week
**Fridays** have the highest incident counts; **Sundays** the lowest. The pattern reflects increased public activity on weekends.

#### Insight 4 — Incidents by Police District
**Mission**, **Southern**, and **Central** districts report the highest volumes, reflecting their dense commercial and residential activity.

#### Insight 5 — Monthly Incident Trend (2018–Present)
Shows a notable dip in early 2020 corresponding to COVID-19 lockdowns, followed by a gradual recovery through 2021–2023.

#### Insight 6 — Resolution Rate by Top 10 Categories
Drug offenses have the highest resolution rates (>60%), while Larceny Theft is frequently unresolved (<20%). The 50% threshold line is included as a reference.

#### Insight 7 — Top 15 Neighborhoods by Incident Count
**Tenderloin** and **Mission** neighborhoods consistently rank highest, driven by population density and commercial concentration.

#### Insight 8 — Heatmap: Hour × Day of Week
A cross-tabulation heatmap showing that **Friday and Saturday afternoons/evenings** (14:00–20:00) are peak incident periods across all districts.

---

## 6. Stage III — Machine Learning

### 6.1 Feature Engineering

All feature transformations are implemented as a PySpark MLlib `Pipeline`:

| Feature | Raw Column | Transformation | Notes |
|---|---|---|---|
| `hour` | `incident_datetime` | `HOUR()` SQL function | 0–23 |
| `day_of_week` | `incident_datetime` | `DAYOFWEEK()` | 1=Sun … 7=Sat |
| `month` | `incident_datetime` | `MONTH()` | 1–12 |
| `year` | `incident_datetime` | Column extraction | 2018–present |
| `latitude` | `latitude` | Direct numeric | Rows with NULL excluded |
| `longitude` | `longitude` | Direct numeric | Rows with NULL excluded |
| `police_district` | `police_district` | `StringIndexer` → `OneHotEncoder` | NULL → "Unknown" |
| **`features`** | All above | `VectorAssembler` + `StandardScaler` | RF and SVC |
| **`features`** (NB) | All above | `VectorAssembler` + `MinMaxScaler` | NB requires non-negative |

**Target variable:** `incident_category` → `StringIndexer` → `label` (top 10 + "Other" = 11 classes)

**Data split:** 70% train / 30% test (random split with seed=42)

### 6.2 Model 1 — Random Forest Classifier

**Implementation:** `pyspark.ml.classification.RandomForestClassifier`

**Hyperparameter grid (27 combinations):**

| Parameter | Values | Description |
|---|---|---|
| `numTrees` | 50, 100, 200 | Number of trees in the forest |
| `maxDepth` | 5, 10, 15 | Maximum depth of each tree |
| `maxBins` | 16, 32, 64 | Number of bins for discretizing features |

**Tuning:** 5-fold `CrossValidator` optimizing weighted F1 score.

**Results:**

| Metric | Value |
|---|---|
| Accuracy | **0.3644** |
| Weighted F1 | **0.2559** |
| Best `numTrees` | 200 |
| Best `maxDepth` | 15 |
| Best `maxBins` | — (default) |

### 6.3 Model 2 — Linear SVC (One-vs-Rest)

**Implementation:** `pyspark.ml.classification.LinearSVC` wrapped in `OneVsRest` for multi-class support.

**Hyperparameter grid (27 combinations):**

| Parameter | Values | Description |
|---|---|---|
| `regParam` | 0.01, 0.1, 1.0 | L2 regularization strength |
| `maxIter` | 50, 100, 200 | Maximum number of iterations |
| `tol` | 1e-4, 1e-3, 1e-2 | Convergence tolerance |

**Tuning:** 5-fold `CrossValidator` optimizing weighted F1 score.

**Results:**

| Metric | Value |
|---|---|
| Accuracy | 0.2701 |
| Weighted F1 | 0.2232 |
| Best `regParam` | 0.1 |
| Best `maxIter` | 50 |

### 6.4 Model 3 — Naive Bayes (Multinomial)

**Implementation:** `pyspark.ml.classification.NaiveBayes` with `modelType="multinomial"`.

Features are scaled with `MinMaxScaler` (instead of `StandardScaler`) to ensure all values are non-negative, as required by Multinomial Naive Bayes.

**Hyperparameter grid (5 combinations):**

| Parameter | Values | Description |
|---|---|---|
| `smoothing` | 0.1, 0.5, 1.0, 1.5, 2.0 | Laplace/Lidstone smoothing |

**Tuning:** 5-fold `CrossValidator` optimizing weighted F1 score.

**Results:**

| Metric | Value |
|---|---|
| Accuracy | 0.3276 |
| Weighted F1 | 0.2248 |
| Best `smoothing` | 1.5 |

### 6.5 Results Summary

| Model | Accuracy | Weighted F1 | CV Folds | Grid Size | Best Params |
|---|---|---|---|---|---|
| Random Forest | **0.3644** | **0.2559** | 5 | 27 | numTrees=200, maxDepth=15 |
| Naive Bayes | 0.3276 | 0.2248 | 5 | 5 | smoothing=1.5 |
| Linear SVC | 0.2701 | 0.2232 | 5 | 27 | regParam=0.1, maxIter=50 |

**Local prototype results (scikit-learn, 10% sample):**

The following results were obtained locally on a 10% stratified sample (52,722 train / 22,596 test) using scikit-learn. Dataset: 753,186 rows after dropping NaN coordinates, 11 classes, 7 features.

| Model | Accuracy | Weighted F1 |
|---|---|---|
| Random Forest (n=100, depth=10) | **0.3656** | **0.2550** |
| Linear SVC (C=0.1) | 0.3315 | 0.2286 |
| Naive Bayes (alpha=1.0) | 0.2915 | 0.1316 |

**Interpretation:** The low metrics reflect severe class imbalance — Larceny Theft alone accounts for ~30% of all records. The Random Forest mostly predicts "Larceny Theft" (recall 0.78) and "Other" (recall 0.57), with near-zero recall for all other categories. This is expected behavior for a baseline model on imbalanced data without resampling. Full Spark MLlib results on the complete dataset with 5-fold cross-validated hyperparameter tuning are expected to yield higher and more balanced scores.

> Feature importance (Random Forest): `Longitude` and `Latitude` are the most predictive features, confirming that crime type is strongly geographically clustered. `hour` and `year` rank next.

### 6.6 Sample Predictions

For each model, 5 sample records from the test set are saved to `output/predictions/<model_name>/`. Each record contains:
- `actual_category` — ground truth label
- `predicted_category` — model prediction
- `hour`, `day_of_week`, `police_district`, `latitude`, `longitude` — input features

---

## 7. Stage IV — Visualization

### 7.1 Superset Export

`scripts/stage4_superset.py` loads all 8 EDA CSV outputs into a local SQLite database (`output/sf_incidents_eda.db`) and generates a Superset v1 export ZIP (`output/superset_export/sf_incidents_dashboard.zip`) containing:

- Database connection YAML (SQLite URI)
- 8 dataset YAMLs (one per EDA insight)
- 8 chart YAMLs (bar, line, and heatmap chart types)
- Dashboard YAML with a 2-column grid layout

The ZIP can be imported directly via **Superset UI → Dashboards → Import dashboard** without any manual configuration. No running Superset instance is required to generate the export.

### 7.2 Streamlit Dashboard

The dashboard (`scripts/stage4_app.py`) consists of 5 pages accessible via the sidebar:

| Page | Content |
|---|---|
| **Data Overview** | Dataset metrics (total incidents, categories, districts), pipeline architecture diagram, feature table |
| **EDA Insights** | Interactive charts for all 8 EDA insights (bar charts, line charts, styled heatmap) |
| **Model Performance** | Accuracy and F1 bar charts for all 3 models, summary table, hyperparameter grid reference |
| **Predictions** | Sample prediction tables per model with correct/incorrect flag |
| **Spatial Map** | `st.map` scatter plot with circle size proportional to incident count per district |

**Run command:**
```bash
streamlit run scripts/stage4_app.py
```

The dashboard reads pre-computed CSV files from `output/eda/` and `output/metrics/`. It does not require a running Spark or Hive instance.

---

## 8. Project Structure

```
bigdata-final-project/
├── data/sf_incidents.csv          # Raw dataset (800K rows, ~500 MB)
├── sql/
│   ├── create_tables.sql          # PostgreSQL DDL (staging + main table)
│   └── create_hive_tables.hql     # Hive external + managed table DDL
├── scripts/
│   ├── preprocess.sh              # Env setup, output dir creation
│   ├── stage1.sh                  # PostgreSQL load + Sqoop → HDFS
│   ├── stage2.sh                  # Hive tables + Spark EDA
│   ├── stage2_eda.py              # PySpark EDA (8 insights, pylint 10/10)
│   ├── stage3.sh                  # Spark ML training (YARN)
│   ├── stage3_ml.py               # PySpark MLlib pipeline (pylint 10/10)
│   ├── stage4.sh                  # Superset export + Streamlit launch
│   ├── stage4_app.py              # Streamlit 5-page dashboard
│   ├── stage4_superset.py         # Superset v1 export ZIP generator
│   └── postprocess.sh             # Output summary
├── notebooks/
│   ├── 01_exploration.ipynb       # Schema, nulls, distributions (pandas)
│   ├── 02_eda.ipynb               # All 8 EDA insights (matplotlib)
│   └── 03_ml_experiments.ipynb   # scikit-learn prototype on 10% sample
├── output/
│   ├── eda/                       # EDA CSV results (one dir per insight)
│   ├── metrics/metrics_summary.json
│   └── predictions/               # Sample predictions per model
├── models/
│   ├── random_forest/             # Saved Spark PipelineModel
│   ├── linear_svc/
│   └── naive_bayes/
├── main.sh                        # Full pipeline runner (grader entry point)
├── requirements.txt               # Python deps
└── .pylintrc                      # Pylint config (10/10 on all scripts)
```

---

## 9. How to Reproduce

### Prerequisites
- Hadoop cluster with YARN
- PostgreSQL (accessible via JDBC)
- Apache Sqoop
- Apache Hive with Tez
- Apache Spark 3.x

### Environment Variables
```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=sf_incidents
export PG_USER=postgres
export PG_PASSWORD=postgres
export HDFS_BASE=/user/$USER/sf_incidents
export HIVE_DB=sf_incidents_db
export SPARK_MASTER=yarn
export DATA_FILE=data/sf_incidents.csv
```

### Run
```bash
# Place dataset at data/sf_incidents.csv first
bash main.sh
```

Individual stages:
```bash
bash scripts/preprocess.sh   # Create output dirs, check tools
bash scripts/stage1.sh       # PostgreSQL + Sqoop
bash scripts/stage2.sh       # Hive + EDA
bash scripts/stage3.sh       # ML training
bash scripts/stage4.sh       # Dashboard
bash scripts/postprocess.sh  # Summary
```

---

## 10. Conclusions

This project demonstrates a complete end-to-end big data pipeline from raw CSV ingestion to interactive visualization:

1. **Data Ingestion** — The two-stage PostgreSQL loading pattern (staging → typed table) proved robust for handling 800K rows with dirty/missing values. Sqoop with Parquet+Snappy delivered efficient columnar storage in HDFS.

2. **EDA Findings** — Temporal analysis confirmed that incidents peak on Friday afternoons and in the afternoon hours. The Tenderloin and Mission neighborhoods are persistent hotspots. The COVID-19 impact is clearly visible in the monthly trend (2020 dip).

3. **ML Performance** — Random Forest (Accuracy 0.3644, F1 0.2559) outperformed both Linear SVC and Naive Bayes, confirming that non-linear decision boundaries between geographic and temporal features are better captured by ensemble tree methods. Linear SVC underperformed due to the OWLQN optimizer encountering numerical instability (NaNHistory resets) with certain hyperparameter combinations, ultimately converging at regParam=0.1. Naive Bayes performed surprisingly close to Random Forest (0.3276 accuracy) given its strong independence assumption, likely because geographic and temporal features carry relatively independent signals.

4. **Feature Importance** — Based on the local scikit-learn prototype, `Longitude`, `Latitude`, and `hour` are the most informative features for predicting incident category, which aligns with the strong geographic clustering of different crime types.

5. **Dashboard** — The Streamlit app provides a self-contained, no-Spark-required view of all pipeline outputs, suitable for stakeholder presentation.

---

---

## 11. Reflections

### Challenges

1. **Sqoop type mapping** — The cluster's Sqoop 1.4.7 (Arenadata edition) maps all time-like PostgreSQL types (`TIMESTAMP`, `DATE`, `TIME`) to INT64 milliseconds in Parquet. The Hive external table DDL must declare these columns as `BIGINT`, not `TIMESTAMP` or `STRING`. Discovering this required reading Sqoop's `_metadata` file to inspect actual Parquet schema.

2. **Hive metastore warehouse location** — Running `CREATE DATABASE IF NOT EXISTS` does not update the warehouse location if the database was previously created with a different `spark.sql.warehouse.dir`. The fix was to unconditionally `DROP DATABASE CASCADE` at the start of each Stage II run.

3. **HDFS vs local filesystem** — PySpark's `df.write.csv("output/eda/...")` with a relative path defaults to HDFS on a YARN cluster, not the local filesystem. Stage IV reads CSVs with Python's `glob`, requiring an explicit `hdfs dfs -get` step after spark-submit.

4. **LinearSVC numerical instability** — The OWLQN optimizer (used by LinearSVC internally) frequently reset its history (`NaNHistory`) during cross-validation folds, especially with large regularization values. This is expected behavior and did not prevent the model from converging.

5. **Streamlit unavailability** — Streamlit was not installed on the cluster. Stage IV is made non-fatal: the Superset export ZIP (which does not require Streamlit) is generated successfully, and Streamlit failure produces a `WARNING` without aborting the pipeline.

### Recommendations

- **Address class imbalance** — Larceny Theft accounts for ~30% of records. Applying SMOTE or class-weighted loss functions could significantly improve recall on minority categories.
- **Richer features** — Adding `incident_subcategory` (encoded), `neighborhood`, and interaction features (latitude × hour) would likely improve model accuracy.
- **Gradient Boosting** — `pyspark.ml.classification.GBTClassifier` would be a strong alternative to Random Forest for this imbalanced tabular dataset.



---

## References

1. [SF PD Incident Reports Dataset — Kaggle](https://www.kaggle.com/datasets/vivovinco/san-francisco-incident-reports-2018present)
2. [Apache Spark MLlib Guide](https://spark.apache.org/docs/latest/ml-guide.html)
3. [Apache Sqoop User Guide](https://sqoop.apache.org/docs/1.4.7/SqoopUserGuide.html)
4. [Apache Hive Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)
5. [Streamlit Documentation](https://docs.streamlit.io)
6. [Course Project Requirements](https://firas-jolha.github.io/bigdata/html/bs/BS%20-%20Final%20Project.html)
