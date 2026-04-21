# SF Police Incident Reports — Big Data Pipeline

## Project Description

This project implements an end-to-end big data pipeline to analyze the **San Francisco Police Department Incident Reports: 2018 to Present** dataset. We identify spatial and temporal patterns of incidents in San Francisco and build multi-class classification models to predict the **incident category** based on time, location, and contextual features.

---

## Dataset

**Source:** [SF Police Department Incident Reports 2018–Present](https://www.kaggle.com/datasets/vivovinco/san-francisco-incident-reports-2018present)

The dataset contains police incidents in San Francisco from 2018 to the present (updated daily). Key attributes:

| Column | Description |
|---|---|
| `Incident Datetime` | Date and time the incident occurred |
| `Report Datetime` | Date and time the report was filed |
| `Incident Year` | Year of the incident |
| `Incident Day of Week` | Day of week (Monday–Sunday) |
| `Incident Category` | **Target variable** — high-level crime category |
| `Incident Subcategory` | More specific crime subcategory |
| `Incident Description` | Free-text description |
| `Police District` | SFPD district where incident occurred |
| `Analysis Neighborhood` | Neighborhood name |
| `Latitude / Longitude` | Geospatial coordinates |
| `Resolution` | Outcome of the incident report |
| `Row ID` | Unique identifier per row |

**Size:** 500,000+ records, 500+ MB — meets the course minimum requirements.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA PIPELINE                            │
│                                                                 │
│  CSV Dataset                                                    │
│      │                                                          │
│      ▼                                                          │
│  ┌──────────────────┐    STAGE I                               │
│  │  PostgreSQL       │  ◄─── Load raw CSV data                 │
│  │  (Citus Data)     │        Create tables with constraints    │
│  └────────┬─────────┘                                          │
│           │ Sqoop (JDBC)                                        │
│           ▼                                                     │
│  ┌──────────────────┐    STAGE I                               │
│  │  HDFS            │  ◄─── Sqoop import (MapReduce)           │
│  │  /user/.../data  │                                          │
│  └────────┬─────────┘                                          │
│           │ Hive External Table                                 │
│           ▼                                                     │
│  ┌──────────────────┐    STAGE II                              │
│  │  Hive            │  ◄─── External table over HDFS           │
│  │  (PARQUET+Snappy)│        Optimized ORC/PARQUET tables       │
│  └────────┬─────────┘        6+ EDA insights (Spark SQL)       │
│           │ Spark SQL                                           │
│           ▼                                                     │
│  ┌──────────────────┐    STAGE III                             │
│  │  Spark MLlib     │  ◄─── Feature engineering                │
│  │  (YARN cluster)  │        Random Forest (tuned)             │
│  │                  │        Linear SVM (tuned)                │
│  │                  │        Naive Bayes (tuned)               │
│  └────────┬─────────┘        K-fold CV, metrics                │
│           │                                                     │
│           ▼                                                     │
│  ┌──────────────────┐    STAGE IV                              │
│  │  Streamlit App   │  ◄─── Results dashboard                  │
│  │  + Superset      │        Metrics, charts, predictions      │
│  └──────────────────┘                                          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Pipeline Stages

### Stage I — Data Collection & Ingestion (10 pts)

**Goal:** Load raw CSV data into PostgreSQL and ingest to HDFS via Sqoop.

**Steps:**
1. Create a distributed PostgreSQL database with proper constraints:
   - Primary key on `row_id`
   - NOT NULL constraints on key columns
   - Check constraints on `incident_year`, `latitude`, `longitude`
2. Load the CSV dataset using `psql COPY` command.
3. Run Sqoop import job (MapReduce) to move data from PostgreSQL to HDFS.

**Deliverables:**
- `sql/create_tables.sql` — DDL for PostgreSQL
- `scripts/stage1.sh` — Sqoop import script
- HDFS directory: `/user/$USER/sf_incidents/raw/`

---

### Stage II — Data Storage & Preparation (15 pts)

**Goal:** Create Hive tables in PARQUET+Snappy format and perform EDA.

**Steps:**
1. Create Hive external table over HDFS Sqoop output.
2. Create optimized PARQUET table with Snappy compression.
3. Perform EDA with **6 data insights** using Spark SQL:

| # | Insight | Description |
|---|---|---|
| 1 | Top 10 Incident Categories | Bar chart of most frequent crime types |
| 2 | Incidents by Hour of Day | Temporal pattern — when crimes peak |
| 3 | Incidents by Day of Week | Weekly crime rhythm |
| 4 | Incidents by Police District | Geographic distribution across SFPD districts |
| 5 | Monthly Trend Over Years | Year-over-year comparison of incident volumes |
| 6 | Resolution Rate by Category | Which categories get resolved most/least often |
| 7 | Top Neighborhoods by Incident Count | Neighborhood-level geographic breakdown |
| 8 | Incident Category Heatmap (Hour × Day) | Combined temporal pattern analysis |

**Deliverables:**
- `sql/create_hive_tables.hql` — Hive DDL
- `scripts/stage2_eda.py` — PySpark SQL EDA script
- `output/eda/` — CSV files with EDA results

---

### Stage III — Data Analysis / ML (23 pts)

**Goal:** Train three classification models to predict `incident_category`.

**Feature Engineering:**
| Feature | Transformation |
|---|---|
| `hour` | Extracted from `incident_datetime` |
| `day_of_week_num` | Monday=0 ... Sunday=6 |
| `month` | Extracted from `incident_datetime` |
| `year` | Extracted from `incident_datetime` |
| `latitude` | Float, imputed with median |
| `longitude` | Float, imputed with median |
| `police_district` | StringIndexer → OneHotEncoder |

**Target:** `incident_category` → StringIndexer (top 10 categories, others = "Other")

**Data split:** 70% train / 30% test

**Models:**

#### Model 1 — Random Forest Classifier (8 pts)
- Hyperparameter grid (3 × 3 × 3 = **27 combinations**):
  - `numTrees`: [50, 100, 200]
  - `maxDepth`: [5, 10, 15]
  - `maxBins`: [16, 32, 64]
- Evaluation: Accuracy + Weighted F1 + 5-fold cross-validation

#### Model 2 — Linear SVM / LinearSVC (8 pts)
- Hyperparameter grid (3 × 3 × 3 = **27 combinations**):
  - `regParam`: [0.01, 0.1, 1.0]
  - `maxIter`: [50, 100, 200]
  - `tol`: [1e-4, 1e-3, 1e-2]
- Evaluation: Accuracy + Weighted F1 + 5-fold cross-validation

#### Model 3 — Naive Bayes Classifier (5 pts)
- Hyperparameter grid:
  - `smoothing`: [0.1, 0.5, 1.0, 1.5, 2.0]
- Evaluation: Accuracy + Weighted F1

**Sample Prediction (2 pts):** Run inference on 5 sample records and output predicted vs actual category.

**Deliverables:**
- `scripts/stage3_ml.py` — full PySpark MLlib pipeline
- `models/` — saved Spark ML models
- `output/metrics/` — JSON/CSV with all model metrics

---

### Stage IV — Visualization / Presentation (15 pts)

**Goal:** Build an interactive Streamlit app that displays pipeline results.

**Dashboard Sections:**
1. **Data Characteristics** — dataset size, schema, feature distributions
2. **EDA Insights** — 6+ charts loaded from `output/eda/`
3. **Model Performance** — accuracy, F1, confusion matrix for all 3 models
4. **Predictions** — sample predictions table
5. **Spatial Map** — folium/pydeck map of incident hotspots

**Note:** An Apache Superset dashboard is also configured (see `output/superset_export/`) to display the same insights with native SQL charts.

**Deliverables:**
- `scripts/stage4_app.py` — Streamlit app
- `output/superset_export/` — Superset dashboard definition

---

## Project Structure

```
bigdata-final-project/
├── data/                       # Raw and processed dataset files
│   └── sf_incidents.csv        # Raw dataset (download from Kaggle)
├── models/                     # Saved Spark ML models
│   ├── random_forest/
│   ├── linear_svc/
│   └── naive_bayes/
├── notebooks/                  # Exploratory Jupyter/Zeppelin notebooks
│   ├── 01_exploration.ipynb
│   ├── 02_eda.ipynb
│   └── 03_ml_experiments.ipynb
├── output/                     # All pipeline outputs
│   ├── eda/                    # EDA result CSVs
│   ├── metrics/                # Model performance JSON/CSV
│   ├── predictions/            # Sample prediction results
│   └── superset_export/        # Superset dashboard files
├── scripts/                    # All pipeline scripts
│   ├── preprocess.sh           # Environment checks and setup
│   ├── stage1.sh               # PostgreSQL + Sqoop ingestion
│   ├── stage2.sh               # Hive tables + Spark EDA
│   ├── stage2_eda.py           # PySpark EDA script
│   ├── stage3.sh               # Spark ML training
│   ├── stage3_ml.py            # PySpark MLlib script
│   ├── stage4.sh               # Streamlit dashboard
│   ├── stage4_app.py           # Streamlit application
│   └── postprocess.sh          # Cleanup and summary
├── sql/                        # SQL and HQL files
│   ├── create_tables.sql       # PostgreSQL DDL
│   └── create_hive_tables.hql  # Hive DDL
├── main.sh                     # Main pipeline runner (DO NOT MODIFY)
├── requirements.txt            # Python dependencies
└── README.MD                   # This file
```

---

## Setup & Configuration

### Prerequisites

- Hadoop cluster with YARN
- PostgreSQL / Citus Data
- Apache Sqoop
- Apache Hive with Tez
- Apache Spark (with YARN support)
- Python 3.8+
- Apache Superset

### Environment Variables

Set these before running `main.sh`:

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=sf_incidents
export PG_USER=postgres
export PG_PASSWORD=your_password

export HDFS_BASE=/user/$USER/sf_incidents
export HIVE_DB=sf_incidents_db

export SPARK_MASTER=yarn
export SPARK_DEPLOY_MODE=cluster
```

### Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Download Dataset

1. Download `sf-incidents.csv` from [Kaggle](https://www.kaggle.com/datasets/vivovinco/san-francisco-incident-reports-2018present)
2. Place it in `data/sf_incidents.csv`

---

## How to Run

```bash
# Run the full pipeline
bash main.sh

# Or run individual stages
bash scripts/preprocess.sh    # Setup
bash scripts/stage1.sh        # PostgreSQL + Sqoop
bash scripts/stage2.sh        # Hive + EDA
bash scripts/stage3.sh        # ML models
bash scripts/stage4.sh        # Dashboard
bash scripts/postprocess.sh   # Cleanup
```

---

## Results Summary

| Model | Accuracy | Weighted F1 |
|---|---|---|
| Random Forest | TBD | TBD |
| Linear SVC | TBD | TBD |
| Naive Bayes | TBD | TBD |

Results will be populated after pipeline execution and stored in `output/metrics/`.

---

## Team

| Name | Contribution |
|---|---|
| Member 1 | Stage I, Stage II |
| Member 2 | Stage III (ML) |
| Member 3 | Stage IV, Report |
| Member 4 | EDA, Documentation |

---

## References

- [SF PD Incident Reports Dataset (Kaggle)](https://www.kaggle.com/datasets/vivovinco/san-francisco-incident-reports-2018present)
- [Apache Spark MLlib](https://spark.apache.org/mllib/)
- [Apache Sqoop](https://sqoop.apache.org/)
- [Apache Hive](https://hive.apache.org/)
- [Course Project Requirements](https://firas-jolha.github.io/bigdata/html/bs/BS%20-%20Final%20Project.html)
