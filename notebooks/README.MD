# notebooks/

Jupyter notebooks used for local exploration and prototyping before implementing the full Spark pipeline.

## Contents

| Notebook | Purpose |
|---|---|
| `01_exploration.ipynb` | Dataset schema, column types, null counts, value distributions |
| `02_eda.ipynb` | All 8 EDA insights reproduced locally with pandas + matplotlib |
| `03_ml_experiments.ipynb` | scikit-learn prototype on 10% stratified sample |

## How to Run

```bash
pip install -r requirements.txt
jupyter notebook notebooks/
```

## Notebook Details

### 01_exploration.ipynb
- Loads `data/sf_incidents.csv` with pandas
- Inspects dtypes, null rates, unique value counts per column
- Identifies columns needing type coercion in PostgreSQL staging

### 02_eda.ipynb
- Reproduces all 8 EDA insights from `scripts/stage2_eda.py` locally
- Uses pandas groupby + matplotlib/seaborn for visualization
- Generates PNG charts saved to `output/eda/plot_*.png`

**Insights:**
1. Top 10 incident categories (bar chart)
2. Incidents by hour of day (bar chart)
3. Incidents by day of week (bar chart)
4. Incidents by police district (horizontal bar + map coordinates)
5. Monthly trend over years (multi-line chart)
6. Resolution rate by category (bar chart)
7. Top 15 neighborhoods by incident count (horizontal bar)
8. Hour × Day heatmap (seaborn heatmap)

### 03_ml_experiments.ipynb
- Loads 10% stratified sample (~75K rows) for fast local iteration
- Feature engineering: hour, day_of_week, month, year, latitude, longitude, police_district
- Target: top 10 incident categories + "Other" (11 classes)
- Trains scikit-learn Random Forest, LinearSVC, and Naive Bayes
- Reports accuracy and weighted F1 as baseline comparison for the Spark models

**Local prototype results (scikit-learn, 10% sample):**

| Model | Accuracy | Weighted F1 |
|---|---|---|
| Random Forest (n=100, depth=10) | 0.3656 | 0.2550 |
| Linear SVC (C=0.1) | 0.3315 | 0.2286 |
| Naive Bayes (alpha=1.0) | 0.2915 | 0.1316 |

These serve as a sanity check. Full Spark MLlib results on the complete cluster dataset are in `output/metrics/`.

## Notes

- Notebooks are for development reference only — they are **not** part of the graded pipeline
- The graded pipeline runs entirely via `bash main.sh` using PySpark on YARN
