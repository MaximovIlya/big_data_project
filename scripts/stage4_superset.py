"""Stage IV (Superset): Build SQLite DB and export Superset v1 dashboard ZIP.

Reads:
  output/eda/          — EDA CSV dirs (stage2)
  output/metrics/      — metrics_summary.json (stage3)
  output/predictions/  — sample prediction CSVs (stage3)

Import: Superset UI -> Dashboards -> (+) -> Import dashboard
"""

import glob
import json
import os
import sqlite3
import zipfile
from datetime import datetime

import pandas as pd

EDA_DIR = "output/eda"
METRICS_DIR = "output/metrics"
PREDICTIONS_DIR = "output/predictions"
DB_PATH = "output/sf_incidents_eda.db"
EXPORT_DIR = "output/superset_export"
ZIP_NAME = "sf_incidents_dashboard.zip"

DB_UUID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
DASH_UUID = "d0000001-0000-0000-0000-000000000001"

_FEATURE_ROWS = [
    {"feature_name": "hour",              "feature_type": "INT",
     "role": "feature", "description": "Hour of incident (0-23)"},
    {"feature_name": "day_of_week",       "feature_type": "STRING",
     "role": "feature", "description": "Day of week (Monday-Sunday)"},
    {"feature_name": "month",             "feature_type": "INT",
     "role": "feature", "description": "Month of year (1-12)"},
    {"feature_name": "year",              "feature_type": "INT",
     "role": "feature", "description": "Year of incident"},
    {"feature_name": "latitude",          "feature_type": "FLOAT",
     "role": "feature", "description": "GPS latitude (WGS84)"},
    {"feature_name": "longitude",         "feature_type": "FLOAT",
     "role": "feature", "description": "GPS longitude (WGS84)"},
    {"feature_name": "police_district",   "feature_type": "STRING",
     "role": "feature", "description": "Police district (StringIndexed + OneHotEncoded)"},
    {"feature_name": "incident_category", "feature_type": "STRING",
     "role": "target",  "description": "Prediction target: top-10 categories + Other (11 classes)"},
]

INSIGHTS = [
    # ── Block 1: Data Characteristics ─────────────────────────────────────
    {
        "key": "summary_stats",
        "source": "csv_dir",
        "table": "summary_stats",
        "title": "Dataset Overview",
        "chart_type": "table",
        "columns": ["total_incidents", "distinct_categories", "distinct_districts"],
        "col_types": {"total_incidents": "INTEGER", "distinct_categories": "INTEGER",
                      "distinct_districts": "INTEGER"},
        "uuid": "c0000009-0000-0000-0000-000000000009",
        "ds_uuid": "s0000009-0000-0000-0000-000000000009",
        "description": (
            "SF PD Incident Reports 2018-present loaded from HDFS via Sqoop. "
            "Counts reflect rows after NULL filtering on incident_category, "
            "latitude, and longitude."
        ),
    },
    {
        "key": "dataset_features",
        "source": "hardcoded",
        "table": "dataset_features",
        "title": "ML Feature Descriptions",
        "chart_type": "table",
        "columns": ["feature_name", "feature_type", "role", "description"],
        "col_types": {"feature_name": "TEXT", "feature_type": "TEXT",
                      "role": "TEXT", "description": "TEXT"},
        "uuid": "c0000010-0000-0000-0000-000000000010",
        "ds_uuid": "s0000010-0000-0000-0000-000000000010",
        "description": (
            "7 input features used for ML classification on YARN. "
            "Categorical features are StringIndexed then OneHotEncoded. "
            "Target is incident_category reduced to top-10 classes plus Other."
        ),
    },
    # ── Block 2: EDA Insights ──────────────────────────────────────────────
    {
        "key": "insight1_top_categories",
        "source": "csv_dir",
        "table": "insight1_top_categories",
        "title": "Top 10 Incident Categories",
        "chart_type": "bar",
        "x_col": "incident_category",
        "y_col": "incident_count",
        "uuid": "c0000001-0000-0000-0000-000000000001",
        "ds_uuid": "s0000001-0000-0000-0000-000000000001",
        "description": (
            "Larceny/Theft dominates with 20%+ of all incidents. "
            "The top 3 categories alone account for nearly half of all reports, "
            "creating a heavily imbalanced classification target that "
            "challenges standard accuracy metrics."
        ),
    },
    {
        "key": "insight2_by_hour",
        "source": "csv_dir",
        "table": "insight2_by_hour",
        "title": "Incidents by Hour of Day",
        "chart_type": "line",
        "x_col": "hour_of_day",
        "y_col": "incident_count",
        "uuid": "c0000002-0000-0000-0000-000000000002",
        "ds_uuid": "s0000002-0000-0000-0000-000000000002",
        "description": (
            "Incident volume rises steadily through the day, peaking 15:00-18:00. "
            "Overnight (02:00-06:00) sees the fewest reports, combining "
            "lower criminal activity with reduced witness reporting. "
            "Hour is one of the strongest individual ML features."
        ),
    },
    {
        "key": "insight3_by_day_of_week",
        "source": "csv_dir",
        "table": "insight3_by_day_of_week",
        "title": "Incidents by Day of Week",
        "chart_type": "bar",
        "x_col": "incident_day_of_week",
        "y_col": "incident_count",
        "uuid": "c0000003-0000-0000-0000-000000000003",
        "ds_uuid": "s0000003-0000-0000-0000-000000000003",
        "description": (
            "Fridays and Saturdays see elevated incident counts driven by "
            "nightlife and increased foot traffic. "
            "Sundays are the quietest day, consistent with reduced "
            "commercial activity and fewer theft opportunities."
        ),
    },
    {
        "key": "insight4_by_district",
        "source": "csv_dir",
        "table": "insight4_by_district",
        "title": "Incidents by Police District",
        "chart_type": "bar",
        "x_col": "police_district",
        "y_col": "incident_count",
        "uuid": "c0000004-0000-0000-0000-000000000004",
        "ds_uuid": "s0000004-0000-0000-0000-000000000004",
        "description": (
            "Southern and Mission districts report the highest volumes, "
            "driven by dense commercial activity and high foot traffic. "
            "Outlying districts such as Richmond and Taraval are significantly "
            "quieter, reflecting suburban residential character."
        ),
    },
    {
        "key": "insight5_monthly_trend",
        "source": "csv_dir",
        "table": "insight5_monthly_trend",
        "title": "Yearly Incident Trend (2018-Present)",
        "chart_type": "line",
        "x_col": "incident_year",
        "y_col": "incident_count",
        "uuid": "c0000005-0000-0000-0000-000000000005",
        "ds_uuid": "s0000005-0000-0000-0000-000000000005",
        "description": (
            "A sharp drop in 2020 reflects COVID-19 lockdown effects on public activity. "
            "Recovery is visible in 2021-2022, but incident levels remain "
            "below pre-pandemic peaks, suggesting lasting behavioral shifts "
            "in how and where San Franciscans spend time."
        ),
    },
    {
        "key": "insight6_resolution_rate",
        "source": "csv_dir",
        "table": "insight6_resolution_rate",
        "title": "Resolution Rate by Category (%)",
        "chart_type": "bar",
        "x_col": "incident_category",
        "y_col": "resolution_rate_pct",
        "uuid": "c0000006-0000-0000-0000-000000000006",
        "ds_uuid": "s0000006-0000-0000-0000-000000000006",
        "description": (
            "Drug and vice offenses have the highest resolution rates as "
            "they typically result in on-scene arrests. "
            "Property crimes (Larceny, Motor Vehicle Theft) resolve at under 20%, "
            "reflecting the inherently low clearance rate for high-volume theft."
        ),
    },
    {
        "key": "insight7_top_neighborhoods",
        "source": "csv_dir",
        "table": "insight7_top_neighborhoods",
        "title": "Top 15 Neighborhoods by Incident Count",
        "chart_type": "bar",
        "x_col": "neighborhood",
        "y_col": "incident_count",
        "uuid": "c0000007-0000-0000-0000-000000000007",
        "ds_uuid": "s0000007-0000-0000-0000-000000000007",
        "description": (
            "Tenderloin, SoMa, and Mission are the top hotspots, shaped by "
            "concentrated poverty, nightlife, and high residential density. "
            "These neighborhoods are the natural priority zones for "
            "predictive resource allocation models."
        ),
    },
    {
        "key": "insight8_hour_day_heatmap",
        "source": "csv_dir",
        "table": "insight8_hour_day_heatmap",
        "title": "Heatmap: Incident Count by Hour x Day of Week",
        "chart_type": "heatmap",
        "x_col": "incident_day_of_week",
        "y_col": "hour_of_day",
        "uuid": "c0000008-0000-0000-0000-000000000008",
        "ds_uuid": "s0000008-0000-0000-0000-000000000008",
        "description": (
            "Friday and Saturday afternoons/evenings are the densest cells. "
            "The interaction of hour x day captures non-linear patterns "
            "that neither feature expresses alone, directly motivating "
            "their joint inclusion in the ML feature set."
        ),
    },
    # ── Block 3: ML Model Performance ─────────────────────────────────────
    {
        "key": "ml_metrics",
        "source": "json",
        "table": "ml_metrics",
        "title": "Model Performance: Accuracy and Weighted F1",
        "chart_type": "grouped_bar",
        "columns": ["model", "accuracy", "weighted_f1"],
        "col_types": {"model": "TEXT", "accuracy": "FLOAT", "weighted_f1": "FLOAT"},
        "uuid": "c0000011-0000-0000-0000-000000000011",
        "ds_uuid": "s0000011-0000-0000-0000-000000000011",
        "description": (
            "Random Forest achieves the best accuracy and weighted F1 among the three models "
            "trained on Apache YARN. "
            "Modest absolute scores reflect the 11-class problem difficulty: "
            "spatiotemporal features alone have limited discriminative power for specific crime type."
        ),
    },
    {
        "key": "ml_metrics_table",
        "source": "json",
        "table": "ml_metrics",
        "title": "Model Metrics Table",
        "chart_type": "table",
        "columns": ["model", "accuracy", "weighted_f1"],
        "col_types": {"model": "TEXT", "accuracy": "FLOAT", "weighted_f1": "FLOAT"},
        "uuid": "c0000013-0000-0000-0000-000000000013",
        "ds_uuid": "s0000011-0000-0000-0000-000000000011",
        "description": "Exact numeric accuracy and weighted F1 scores for all three models.",
    },
    # ── Block 4: Prediction Results ────────────────────────────────────────
    {
        "key": "ml_predictions",
        "source": "predictions",
        "table": "ml_predictions",
        "title": "Sample Predictions: Actual vs Predicted",
        "chart_type": "table",
        "columns": ["model", "actual_category", "predicted_category",
                    "hour", "day_of_week", "police_district"],
        "col_types": {"model": "TEXT", "actual_category": "TEXT",
                      "predicted_category": "TEXT", "hour": "INTEGER",
                      "day_of_week": "TEXT", "police_district": "TEXT"},
        "uuid": "c0000012-0000-0000-0000-000000000012",
        "ds_uuid": "s0000012-0000-0000-0000-000000000012",
        "description": (
            "5 sample predictions per model drawn from the 30% holdout test set. "
            "Rows where actual_category equals predicted_category are correct. "
            "Most misclassifications occur between similar or spatially co-located crime types."
        ),
    },
]


def load_csv_dir(path):
    """Load a Spark-output CSV directory (one or more part files) into a DataFrame."""
    files = glob.glob(os.path.join(path, "*.csv"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)


def _load_metrics_json():
    """Read output/metrics/metrics_summary.json → DataFrame with model/accuracy/weighted_f1."""
    path = os.path.join(METRICS_DIR, "metrics_summary.json")
    if not os.path.exists(path):
        return pd.DataFrame()
    with open(path, encoding="utf-8") as fh:
        metrics = json.load(fh)
    rows = []
    for model_name in ["random_forest", "linear_svc", "naive_bayes"]:
        entry = metrics.get(model_name)
        if isinstance(entry, dict):
            rows.append({
                "model": model_name.replace("_", " ").title(),
                "accuracy": round(entry["accuracy"], 4),
                "weighted_f1": round(entry["weighted_f1"], 4),
            })
    return pd.DataFrame(rows)


def _load_predictions():
    """Combine sample prediction CSVs from all three models into one DataFrame."""
    dfs = []
    for model_name in ["random_forest", "linear_svc", "naive_bayes"]:
        df = load_csv_dir(os.path.join(PREDICTIONS_DIR, model_name))
        if not df.empty:
            df.insert(0, "model", model_name.replace("_", " ").title())
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _source_dataframe(ins):
    """Return the DataFrame for an insight based on its source type."""
    source = ins.get("source", "csv_dir")
    if source == "csv_dir":
        return load_csv_dir(os.path.join(EDA_DIR, ins["key"]))
    if source == "hardcoded":
        return pd.DataFrame(_FEATURE_ROWS)
    if source == "json":
        return _load_metrics_json()
    if source == "predictions":
        return _load_predictions()
    return pd.DataFrame()


def build_sqlite(insights):
    """Load all insight data into SQLite; return list of successfully loaded insights."""
    loaded = []
    loaded_tables = set()
    con = sqlite3.connect(DB_PATH)
    for ins in insights:
        table = ins["table"]
        # Reuse already-written table (e.g. ml_metrics_table shares ml_metrics)
        if table in loaded_tables:
            print(f"  [REUSE] {ins['key']} → table {table} already loaded")
            loaded.append(ins)
            continue
        df = _source_dataframe(ins)
        if df is None or df.empty:
            print(f"  [SKIP]  {ins['key']} — no data (run stage2/stage3 first)")
            continue
        df.to_sql(table, con, if_exists="replace", index=False)
        loaded_tables.add(table)
        print(f"  [OK]    {ins['key']} → {len(df)} rows → table '{table}'")
        loaded.append(ins)
    con.close()
    return loaded


# ── Superset YAML generators ──────────────────────────────────────────────────

def db_yaml():
    """Superset database YAML (SQLite) — Superset 3.x format."""
    abs_db = os.path.abspath(DB_PATH).replace("\\", "/")
    return f"""database_name: SF Incidents EDA
sqlalchemy_uri: sqlite:///{abs_db}
cache_timeout: null
expose_in_sqllab: true
allow_run_async: false
allow_ctas: false
allow_cvas: false
allow_dml: false
force_ctas_schema: null
allow_multi_schema_metadata_fetch: false
impersonate_user: false
encrypted_extra: null
extra: null
server_cert: null
is_managed_externally: false
external_url: null
uuid: {DB_UUID}
version: 1.0.0
"""


def _col_yaml(col_name, col_type="TEXT", is_num=False):
    """Single column entry for dataset YAML."""
    return (
        f"- column_name: {col_name}\n"
        f"  verbose_name: {col_name.replace('_', ' ').title()}\n"
        f"  is_dttm: false\n"
        f"  is_active: true\n"
        f"  type: {col_type}\n"
        f"  groupby: {str(not is_num).lower()}\n"
        f"  filterable: true\n"
        f"  expression: ''\n"
        f"  description: null\n"
        f"  python_date_format: null\n"
        f"  extra: null"
    )


def dataset_yaml(ins):
    """Superset dataset YAML for one insight."""
    if "columns" in ins:
        col_types = ins.get("col_types", {})
        numeric = {"INTEGER", "FLOAT", "INT", "BIGINT", "DOUBLE"}
        col_entries = [
            _col_yaml(c, col_types.get(c, "TEXT"),
                      is_num=col_types.get(c, "TEXT").upper() in numeric)
            for c in ins["columns"]
        ]
        columns_block = "columns:\n" + "\n".join(col_entries)
    else:
        x_col, y_col = ins["x_col"], ins["y_col"]
        columns_block = (
            "columns:\n"
            + _col_yaml(x_col, "TEXT", is_num=False) + "\n"
            + _col_yaml(y_col, "FLOAT", is_num=True)
        )

    return f"""table_name: {ins['table']}
main_dttm_col: null
description: null
default_endpoint: null
offset: 0
cache_timeout: null
schema: null
sql: null
params: null
template_params: null
filter_select_enabled: false
fetch_values_predicate: null
extra: null
normalize_columns: false
always_filter_main_dttm: false
is_managed_externally: false
external_url: null
uuid: {ins['ds_uuid']}
metrics:
- metric_name: count
  verbose_name: Count
  metric_type: count
  expression: COUNT(*)
  description: null
  d3format: null
  extra: null
  warning_text: null
  currency: null
{columns_block}
database_uuid: {DB_UUID}
version: 1.0.0
"""


def chart_params(ins):
    """Return (viz_type, params_json_string) for a chart insight."""
    ctype = ins["chart_type"]

    if ctype == "table":
        viz_type = "table"
        params = {
            "viz_type": "table",
            "query_mode": "raw",
            "all_columns": ins.get("columns", []),
            "row_limit": 100,
            "order_by_cols": [],
            "include_search": False,
            "show_cell_bars": False,
        }

    elif ctype == "grouped_bar":
        viz_type = "bar"
        params = {
            "viz_type": "bar",
            "metrics": [
                {"expressionType": "SIMPLE",
                 "column": {"column_name": "accuracy"},
                 "aggregate": "MAX", "label": "Accuracy"},
                {"expressionType": "SIMPLE",
                 "column": {"column_name": "weighted_f1"},
                 "aggregate": "MAX", "label": "Weighted F1"},
            ],
            "groupby": ["model"],
            "bar_stacked": False,
            "show_legend": True,
            "show_bar_value": True,
            "rich_tooltip": True,
            "order_bars": False,
        }

    elif ctype == "line":
        x_col, y_col = ins["x_col"], ins["y_col"]
        viz_type = "echarts_timeseries_line"
        params = {
            "viz_type": "echarts_timeseries_line",
            "x_axis": x_col,
            "time_grain_sqla": None,
            "metrics": [{"expressionType": "SIMPLE",
                         "column": {"column_name": y_col},
                         "aggregate": "SUM", "label": y_col}],
            "groupby": [],
            "rich_tooltip": True,
            "show_legend": True,
        }

    elif ctype == "heatmap":
        x_col, y_col = ins["x_col"], ins["y_col"]
        viz_type = "heatmap"
        params = {
            "viz_type": "heatmap",
            "all_columns_x": x_col,
            "all_columns_y": y_col,
            "metric": {"expressionType": "SIMPLE",
                       "column": {"column_name": "incident_count"},
                       "aggregate": "SUM", "label": "incident_count"},
            "linear_color_scheme": "oranges",
            "xscale_interval": 1,
            "yscale_interval": 1,
            "canvas_image_rendering": "auto",
            "normalize_across": "heatmap",
            "left_margin": "auto",
            "bottom_margin": "auto",
        }

    else:  # bar (default)
        x_col, y_col = ins["x_col"], ins["y_col"]
        viz_type = "bar"
        params = {
            "viz_type": "bar",
            "metrics": [{"expressionType": "SIMPLE",
                         "column": {"column_name": y_col},
                         "aggregate": "SUM", "label": y_col}],
            "groupby": [x_col],
            "row_limit": 50,
            "bar_stacked": False,
            "show_legend": False,
            "show_bar_value": True,
            "rich_tooltip": True,
            "order_bars": True,
        }

    return viz_type, json.dumps(params)


def chart_yaml(ins):
    """Superset chart YAML."""
    viz_type, params_json = chart_params(ins)
    title_escaped = ins['title'].replace("'", "''")
    params_escaped = params_json.replace("'", "''")
    return f"""slice_name: '{title_escaped}'
description: null
certified_by: null
certification_details: null
is_managed_externally: false
external_url: null
query_context: null
viz_type: {viz_type}
params: '{params_escaped}'
cache_timeout: null
uuid: {ins['uuid']}
dataset_uuid: {ins['ds_uuid']}
version: 1.0.0
"""


def dashboard_yaml(loaded_insights):
    """Superset dashboard YAML with a 2-column grid layout."""
    position = {
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [],
            "parents": ["ROOT_ID"],
        }
    }

    for i, ins in enumerate(loaded_insights):
        col = i % 2
        row_idx = i // 2
        row_key = f"ROW-{row_idx}"
        chart_key = f"CHART-{i}"

        if col == 0:
            position[row_key] = {
                "type": "ROW",
                "id": row_key,
                "children": [],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "parents": ["ROOT_ID", "GRID_ID"],
            }
            position["GRID_ID"]["children"].append(row_key)

        position[chart_key] = {
            "type": "CHART",
            "id": chart_key,
            "children": [],
            "meta": {
                "chartId": ins["uuid"],
                "width": 6,
                "height": 50,
                "sliceName": ins["title"],
            },
            "parents": ["ROOT_ID", "GRID_ID", row_key],
        }
        position[row_key]["children"].append(chart_key)

    chart_refs = "\n".join(f"- uuid: {ins['uuid']}" for ins in loaded_insights)

    return f"""dashboard_title: SF Police Incidents - Big Data Pipeline
description: null
certified_by: null
certification_details: null
is_managed_externally: false
external_url: null
css: ''
slug: sf-incidents-pipeline
uuid: {DASH_UUID}
position: {json.dumps(position)}
metadata:
  native_filter_configuration: []
  timed_refresh_immune_slices: []
  expanded_slices: {{}}
  refresh_frequency: 0
  default_filters: '{{}}'
  color_scheme: ''
  color_namespace: ''
  label_colors: {{}}
  shared_label_colors: {{}}
  filter_scopes: {{}}
  cross_filters_enabled: false
version: 1.0.0
charts:
{chart_refs}
"""


def write_export(loaded_insights):
    """Write Superset v1 export ZIP."""
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    root = f"dashboard_export_{ts}"

    os.makedirs(EXPORT_DIR, exist_ok=True)
    zip_path = os.path.join(EXPORT_DIR, ZIP_NAME)

    # Track written dataset UUIDs to avoid duplicates
    written_datasets = set()

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{root}/metadata.yaml",
                    f"version: 1.0.0\ntype: Dashboard\ntimestamp: '{ts}'\n")
        zf.writestr(f"{root}/databases/SF_Incidents_EDA.yaml", db_yaml())

        for ins in loaded_insights:
            if ins["ds_uuid"] not in written_datasets:
                zf.writestr(
                    f"{root}/datasets/SF_Incidents_EDA/{ins['table']}.yaml",
                    dataset_yaml(ins),
                )
                written_datasets.add(ins["ds_uuid"])

            safe = ins["title"].replace(" ", "_").replace("/", "-").replace(":", "")
            zf.writestr(f"{root}/charts/{safe}.yaml", chart_yaml(ins))

        zf.writestr(
            f"{root}/dashboards/SF_Incidents_Dashboard.yaml",
            dashboard_yaml(loaded_insights),
        )

    print(f"\nSuperset export -> {zip_path}")
    print("Import: Superset UI -> Dashboards -> (+) -> Import dashboard")
    return zip_path


def main():
    """Build SQLite DB from all sources and generate Superset export ZIP."""
    os.makedirs(EXPORT_DIR, exist_ok=True)

    print("=== Stage IV (Superset): loading data into SQLite ===")
    loaded = build_sqlite(INSIGHTS)

    if not loaded:
        print("\nNo data found — run stage2 and stage3 first.")
        print("Generating empty export template...")
        loaded = INSIGHTS

    print(f"\nLoaded {len(loaded)}/{len(INSIGHTS)} insights into {DB_PATH}")

    print("\n=== Generating Superset v1 export ZIP ===")
    write_export(loaded)
    print("\nDone.")


if __name__ == "__main__":
    main()
