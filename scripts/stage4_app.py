"""Stage IV: Streamlit dashboard for SF Police Incident Reports pipeline results.

Reads pre-computed CSVs from output/eda/ and output/metrics/.
Does NOT connect to Spark or Hive at runtime.
"""

import glob
import json
import os

import pandas as pd
import streamlit as st

# ── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SF Incidents — Big Data Pipeline",
    page_icon="🚔",
    layout="wide",
)

EDA_DIR = "output/eda"
METRICS_DIR = "output/metrics"
PREDICTIONS_DIR = "output/predictions"


def load_csv_dir(path):
    """Load a Spark-output CSV directory (contains part-*.csv) into a DataFrame."""
    files = glob.glob(os.path.join(path, "*.csv"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)


def load_metrics():
    """Load model metrics summary JSON."""
    path = os.path.join(METRICS_DIR, "metrics_summary.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as file_handle:
        return json.load(file_handle)


# ── Sidebar ────────────────────────────────────────────────────────────────
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Go to",
    ["Data Overview", "EDA Insights", "Model Performance", "Predictions", "Spatial Map"],
)

# ══════════════════════════════════════════════════════════════════════════════
# Page 1 — Data Overview
# ══════════════════════════════════════════════════════════════════════════════
if page == "Data Overview":
    st.title("🚔 SF Police Incident Reports — Big Data Pipeline")
    st.markdown(
        """
        **Dataset:** San Francisco PD Incident Reports 2018–Present
        **Source:** [Kaggle](https://www.kaggle.com/datasets/vivovinco/san-francisco-incident-reports-2018present)
        **Goal:** Predict `incident_category` using time, location, and district features.
        """
    )

    col1, col2, col3 = st.columns(3)

    summary = load_csv_dir(os.path.join(EDA_DIR, "summary_stats"))
    if not summary.empty:
        col1.metric("Total Incidents", f"{int(summary['total_incidents'].iloc[0]):,}")
        col2.metric("Incident Categories", int(summary['distinct_categories'].iloc[0]))
        col3.metric("Police Districts", int(summary['distinct_districts'].iloc[0]))
    else:
        col1.metric("Total Incidents", "Run pipeline to see")
        col2.metric("Incident Categories", "—")
        col3.metric("Police Districts", "—")

    st.divider()
    st.subheader("Pipeline Architecture")
    st.code(
        "CSV → PostgreSQL → HDFS (Sqoop) → Hive PARQUET+Snappy → Spark SQL EDA"
        " → Spark MLlib (RF / SVC / NB) → Streamlit",
        language="text",
    )

    st.subheader("Features Used for Classification")
    feat_df = pd.DataFrame({
        "Feature": ["hour", "day_of_week", "month", "year",
                    "latitude", "longitude", "police_district"],
        "Type": ["numeric", "numeric", "numeric", "numeric",
                 "numeric", "numeric", "categorical (OHE)"],
        "Description": [
            "Hour of incident (0–23)",
            "Day of week (1=Sun … 7=Sat)",
            "Month (1–12)",
            "Incident year",
            "Latitude (37.0–38.5)",
            "Longitude (−123.5 to −122.0)",
            "SFPD patrol district",
        ],
    })
    st.dataframe(feat_df, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# Page 2 — EDA Insights
# ══════════════════════════════════════════════════════════════════════════════
elif page == "EDA Insights":
    st.title("📊 EDA Insights")

    # Insight 1 — Top categories
    st.subheader("1. Top 10 Incident Categories")
    df1 = load_csv_dir(os.path.join(EDA_DIR, "insight1_top_categories"))
    if not df1.empty:
        df1 = df1.sort_values("incident_count", ascending=False)
        st.bar_chart(df1.set_index("incident_category")["incident_count"])
        st.dataframe(df1, use_container_width=True)
        st.caption(
            "Larceny Theft is typically the most reported category, followed by "
            "Other Miscellaneous and Non-Criminal incidents."
        )
    else:
        st.info("Run stage2 to generate EDA results.")

    st.divider()

    # Insight 2 — By hour
    st.subheader("2. Incidents by Hour of Day")
    df2 = load_csv_dir(os.path.join(EDA_DIR, "insight2_by_hour"))
    if not df2.empty:
        df2 = df2.sort_values("hour_of_day")
        st.line_chart(df2.set_index("hour_of_day")["incident_count"])
        st.caption(
            "Incidents peak in the afternoon (noon–6 PM) and drop sharply "
            "between 3–6 AM, reflecting typical urban crime patterns."
        )

    st.divider()

    # Insight 3 — By day of week
    st.subheader("3. Incidents by Day of Week")
    df3 = load_csv_dir(os.path.join(EDA_DIR, "insight3_by_day_of_week"))
    if not df3.empty:
        st.bar_chart(df3.set_index("incident_day_of_week")["incident_count"])
        st.caption(
            "Fridays and Saturdays tend to have more incidents, "
            "while Sundays typically see fewer reports."
        )

    st.divider()

    # Insight 4 — By district
    st.subheader("4. Incidents by Police District")
    df4 = load_csv_dir(os.path.join(EDA_DIR, "insight4_by_district"))
    if not df4.empty:
        df4 = df4.sort_values("incident_count", ascending=False)
        st.bar_chart(df4.set_index("police_district")["incident_count"])
        st.caption(
            "The Mission, Southern, and Central districts consistently "
            "report the highest incident volumes."
        )

    st.divider()

    # Insight 5 — Monthly trend
    st.subheader("5. Monthly Incident Trend Over Years")
    df5 = load_csv_dir(os.path.join(EDA_DIR, "insight5_monthly_trend"))
    if not df5.empty:
        year_str = df5["incident_year"].astype(str)
        month_str = df5["month"].astype(str).str.zfill(2)
        df5["period"] = year_str + "-" + month_str
        st.line_chart(df5.set_index("period")["incident_count"])
        st.caption(
            "A notable dip in early 2020 corresponds to COVID-19 lockdowns, "
            "followed by a gradual recovery."
        )

    st.divider()

    # Insight 6 — Resolution rate
    st.subheader("6. Resolution Rate by Category")
    df6 = load_csv_dir(os.path.join(EDA_DIR, "insight6_resolution_rate"))
    if not df6.empty:
        df6 = df6.sort_values("resolution_rate_pct", ascending=False)
        st.bar_chart(df6.set_index("incident_category")["resolution_rate_pct"])
        st.caption(
            "Drug/Narcotic offences tend to have higher resolution rates "
            "while Larceny Theft is frequently left open."
        )

    st.divider()

    # Insight 7 — Top neighborhoods
    st.subheader("7. Top 15 Neighborhoods by Incident Count")
    df7 = load_csv_dir(os.path.join(EDA_DIR, "insight7_top_neighborhoods"))
    if not df7.empty:
        df7 = df7.sort_values("incident_count", ascending=False)
        st.bar_chart(df7.set_index("neighborhood")["incident_count"])
        st.caption(
            "The Tenderloin and Mission neighborhoods report the most incidents "
            "due to their dense population and commercial activity."
        )

    st.divider()

    # Insight 8 — Heatmap hour × day
    st.subheader("8. Incident Heatmap — Hour × Day of Week")
    df8 = load_csv_dir(os.path.join(EDA_DIR, "insight8_hour_day_heatmap"))
    if not df8.empty:
        pivot = df8.pivot_table(
            index="hour_of_day", columns="incident_day_of_week",
            values="incident_count", aggfunc="sum",
        )
        st.dataframe(pivot.style.background_gradient(cmap="YlOrRd"), use_container_width=True)
        st.caption(
            "The heatmap reveals that Friday and Saturday afternoons/evenings "
            "are the peak periods for incidents across the city."
        )

# ══════════════════════════════════════════════════════════════════════════════
# Page 3 — Model Performance
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Model Performance":
    st.title("🤖 Model Performance")

    metrics = load_metrics()
    if metrics is None:
        st.info("Run stage3 to generate model metrics.")
        st.stop()

    cfg = metrics.get("config", {})
    st.markdown(
        f"**Target classes:** top {cfg.get('top_n_categories', '—')} categories + Other  |  "
        f"**Split:** {int(cfg.get('train_ratio', 0.7)*100)}% train / "
        f"{int(cfg.get('test_ratio', 0.3)*100)}% test  |  "
        f"**CV folds:** {cfg.get('cv_folds', '—')}"
    )

    rows = [
        {"Model": k, "Accuracy": v["accuracy"], "Weighted F1": v["weighted_f1"]}
        for k, v in metrics.items()
        if k != "config"
    ]
    results_df = pd.DataFrame(rows).sort_values("Weighted F1", ascending=False)

    col1, col2 = st.columns(2)
    col1.subheader("Accuracy")
    col1.bar_chart(results_df.set_index("Model")["Accuracy"])
    col2.subheader("Weighted F1")
    col2.bar_chart(results_df.set_index("Model")["Weighted F1"])

    st.subheader("Summary Table")
    st.dataframe(
        results_df.style.format({"Accuracy": "{:.4f}", "Weighted F1": "{:.4f}"}),
        use_container_width=True,
    )

    st.subheader("Hyperparameter Grids")
    st.markdown("""
| Model | Param | Values | Combinations |
|---|---|---|---|
| Random Forest | `numTrees` | 50, 100, 200 | |
| | `maxDepth` | 5, 10, 15 | |
| | `maxBins` | 16, 32, 64 | **27** |
| LinearSVC | `regParam` | 0.01, 0.1, 1.0 | |
| | `maxIter` | 50, 100, 200 | |
| | `tol` | 1e-4, 1e-3, 1e-2 | **27** |
| Naive Bayes | `smoothing` | 0.1, 0.5, 1.0, 1.5, 2.0 | **5** |
    """)

# ══════════════════════════════════════════════════════════════════════════════
# Page 4 — Predictions
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Predictions":
    st.title("🔮 Sample Predictions")
    st.markdown("5 sample records from the test set — actual vs predicted category.")

    for model_name in ["random_forest", "linear_svc", "naive_bayes"]:
        st.subheader(model_name.replace("_", " ").title())
        preds_path = os.path.join(PREDICTIONS_DIR, model_name)
        df = load_csv_dir(preds_path)
        if df.empty:
            st.info(f"No predictions yet for {model_name}. Run stage3.")
        else:
            df["correct"] = df["actual_category"] == df["predicted_category"]
            st.dataframe(df, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# Page 5 — Spatial Map
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Spatial Map":
    st.title("🗺️ Spatial Distribution of Incidents")

    df4 = load_csv_dir(os.path.join(EDA_DIR, "insight4_by_district"))
    if df4.empty:
        st.info("Run stage2 to generate district data.")
        st.stop()

    # District centroids for map approximation
    district_coords = {
        "Bayview":    (37.7314, -122.3900),
        "Central":    (37.7989, -122.4100),
        "Ingleside":  (37.7243, -122.4457),
        "Mission":    (37.7599, -122.4148),
        "Northern":   (37.7800, -122.4320),
        "Park":       (37.7672, -122.4545),
        "Richmond":   (37.7793, -122.4837),
        "Southern":   (37.7765, -122.3940),
        "Taraval":    (37.7433, -122.4817),
        "Tenderloin": (37.7832, -122.4125),
    }

    map_rows = []
    for _, row in df4.iterrows():
        district = row.get("police_district", "")
        coords = district_coords.get(district, (float(row.get("avg_lat", 37.77)),
                                                float(row.get("avg_lon", -122.42))))
        map_rows.append({
            "lat": coords[0],
            "lon": coords[1],
            "size": float(row["incident_count"]),
            "district": district,
        })

    map_df = pd.DataFrame(map_rows)
    st.map(map_df[["lat", "lon"]], size="size")

    st.caption(
        "Circle size is proportional to incident count. "
        "Southern and Mission districts dominate the incident map."
    )
    st.dataframe(df4[["police_district", "incident_count"]].sort_values(
        "incident_count", ascending=False), use_container_width=True)
