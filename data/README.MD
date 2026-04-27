# data/

This directory contains the raw dataset used by the pipeline.

## Dataset

**Name:** San Francisco Police Department Incident Reports: 2018 to Present  
**Source:** [Kaggle](https://www.kaggle.com/datasets/vivovinco/san-francisco-incident-reports-2018present)  
**File:** `sf_incidents.csv`  
**Size:** ~500 MB, 796,456 rows, 35 columns  
**Time range:** 2018-01-01 — present (updated daily)

## Setup

The dataset file is excluded from the repository (listed in `.gitignore`). Download and place it here before running the pipeline:

```bash
# Option 1: Kaggle CLI
kaggle datasets download vivovinco/san-francisco-incident-reports-2018present
unzip san-francisco-incident-reports-2018present.zip -d data/
mv data/Police_Department_Incident_Reports__2018_to_Present.csv data/sf_incidents.csv

# Option 2: Manual download
# Download from Kaggle and rename to data/sf_incidents.csv
```

## Key Columns

| Column | Type | Description |
|---|---|---|
| `Row ID` | INTEGER | Unique incident identifier (primary key) |
| `Incident Datetime` | TIMESTAMP | Date and time the incident occurred |
| `Incident Year` | SMALLINT | Year of incident (2018+) |
| `Incident Day of Week` | TEXT | Day name (Monday–Sunday) |
| `Incident Category` | TEXT | Target variable — high-level crime type (60+ values) |
| `Incident Subcategory` | TEXT | More specific crime subcategory |
| `Police District` | TEXT | SFPD patrol district (10 districts + Out of SF) |
| `Analysis Neighborhood` | TEXT | Neighborhood name |
| `Latitude` | FLOAT | Geographic latitude (37.0–38.5°N) |
| `Longitude` | FLOAT | Geographic longitude (−123.5 to −122.0°W) |
| `Resolution` | TEXT | Outcome of the incident (e.g., Arrest, Cite, Open) |

## Data Quality Notes

- ~5% of rows have missing `Latitude`/`Longitude` — excluded from ML but kept in EDA
- ~1% have missing `Police District` — replaced with `"Unknown"` in ML pipeline
- Dirty rows (type mismatches) are handled via staging table pattern in PostgreSQL
- 795,743 rows successfully loaded after deduplication (`ON CONFLICT DO NOTHING`)
