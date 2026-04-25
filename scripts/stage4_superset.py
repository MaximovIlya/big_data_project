"""Stage IV (Superset): Build SQLite DB from EDA CSVs and export Superset dashboard ZIP.

The ZIP follows Superset v1 export format and can be imported via:
  Superset UI → Dashboards → Import dashboard → select the ZIP file

No running Superset instance is required to generate the export.
"""

import glob
import json
import os
import sqlite3
import zipfile
from datetime import datetime

import pandas as pd

EDA_DIR = "output/eda"
DB_PATH = "output/sf_incidents_eda.db"
EXPORT_DIR = "output/superset_export"
ZIP_NAME = "sf_incidents_dashboard.zip"

DB_UUID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
DASH_UUID = "d0000001-0000-0000-0000-000000000001"

INSIGHTS = [
    {
        "key": "insight1_top_categories",
        "table": "insight1_top_categories",
        "title": "Top 10 Incident Categories",
        "chart_type": "bar",
        "x_col": "incident_category",
        "y_col": "incident_count",
        "uuid": "c0000001-0000-0000-0000-000000000001",
        "ds_uuid": "s0000001-0000-0000-0000-000000000001",
    },
    {
        "key": "insight2_by_hour",
        "table": "insight2_by_hour",
        "title": "Incidents by Hour of Day",
        "chart_type": "line",
        "x_col": "hour_of_day",
        "y_col": "incident_count",
        "uuid": "c0000002-0000-0000-0000-000000000002",
        "ds_uuid": "s0000002-0000-0000-0000-000000000002",
    },
    {
        "key": "insight3_by_day_of_week",
        "table": "insight3_by_day_of_week",
        "title": "Incidents by Day of Week",
        "chart_type": "bar",
        "x_col": "incident_day_of_week",
        "y_col": "incident_count",
        "uuid": "c0000003-0000-0000-0000-000000000003",
        "ds_uuid": "s0000003-0000-0000-0000-000000000003",
    },
    {
        "key": "insight4_by_district",
        "table": "insight4_by_district",
        "title": "Incidents by Police District",
        "chart_type": "bar",
        "x_col": "police_district",
        "y_col": "incident_count",
        "uuid": "c0000004-0000-0000-0000-000000000004",
        "ds_uuid": "s0000004-0000-0000-0000-000000000004",
    },
    {
        "key": "insight5_monthly_trend",
        "table": "insight5_monthly_trend",
        "title": "Monthly Incident Trend",
        "chart_type": "line",
        "x_col": "incident_year",
        "y_col": "incident_count",
        "uuid": "c0000005-0000-0000-0000-000000000005",
        "ds_uuid": "s0000005-0000-0000-0000-000000000005",
    },
    {
        "key": "insight6_resolution_rate",
        "table": "insight6_resolution_rate",
        "title": "Resolution Rate by Category (%)",
        "chart_type": "bar",
        "x_col": "incident_category",
        "y_col": "resolution_rate_pct",
        "uuid": "c0000006-0000-0000-0000-000000000006",
        "ds_uuid": "s0000006-0000-0000-0000-000000000006",
    },
    {
        "key": "insight7_top_neighborhoods",
        "table": "insight7_top_neighborhoods",
        "title": "Top 15 Neighborhoods by Incident Count",
        "chart_type": "bar",
        "x_col": "neighborhood",
        "y_col": "incident_count",
        "uuid": "c0000007-0000-0000-0000-000000000007",
        "ds_uuid": "s0000007-0000-0000-0000-000000000007",
    },
    {
        "key": "insight8_hour_day_heatmap",
        "table": "insight8_hour_day_heatmap",
        "title": "Heatmap: Hour × Day of Week",
        "chart_type": "heatmap",
        "x_col": "incident_day_of_week",
        "y_col": "hour_of_day",
        "uuid": "c0000008-0000-0000-0000-000000000008",
        "ds_uuid": "s0000008-0000-0000-0000-000000000008",
    },
]


def load_csv_dir(path):
    """Load a Spark-output CSV directory into a DataFrame."""
    files = glob.glob(os.path.join(path, "*.csv"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)


def build_sqlite(insights):
    """Load EDA CSVs into SQLite database."""
    loaded = []
    con = sqlite3.connect(DB_PATH)
    for ins in insights:
        path = os.path.join(EDA_DIR, ins["key"])
        insight_df = load_csv_dir(path)
        if insight_df.empty:
            print(f"  [SKIP] {ins['key']} — no CSV found (run stage2 first)")
            continue
        insight_df.to_sql(ins["table"], con, if_exists="replace", index=False)
        print(f"  [OK]   {ins['key']} → {len(df)} rows")
        loaded.append(ins)
    con.close()
    return loaded


def db_yaml():
    """Generate Superset database YAML."""
    abs_db = os.path.abspath(DB_PATH).replace("\\", "/")
    extra = (
        '{"metadata_params":{},"engine_params":{},'
        '"metadata_cache_timeout":{},"schemas_allowed_for_csv_upload":[]}'
    )
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
encrypted_extra: '{{}}'
extra: '{extra}'
server_cert: null
uuid: {DB_UUID}
version: 1.0.0
"""


def dataset_yaml(ins):
    """Generate Superset dataset YAML for one insight."""
    x_col = ins["x_col"]
    y_col = ins["y_col"]
    columns_block = f"""columns:
- column_name: {x_col}
  verbose_name: {x_col.replace('_', ' ').title()}
  is_dttm: false
  is_active: true
  type: TEXT
  groupby: true
  filterable: true
  expression: ''
  description: null
  python_date_format: null
  extra: '{{}}'
- column_name: {y_col}
  verbose_name: {y_col.replace('_', ' ').title()}
  is_dttm: false
  is_active: true
  type: FLOAT
  groupby: false
  filterable: true
  expression: ''
  description: null
  python_date_format: null
  extra: '{{}}'"""

    return f"""table_name: {ins['table']}
main_dttm_col: null
description: {ins['title']}
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
uuid: {ins['ds_uuid']}
metrics:
- metric_name: count
  verbose_name: Count
  metric_type: count
  expression: COUNT(*)
  description: null
  d3format: null
  extra: '{{}}'
  warning_text: null
{columns_block}
database_uuid: {DB_UUID}
version: 1.0.0
"""


def chart_params(ins):
    """Return viz_type and params JSON for a chart."""
    x_col = ins["x_col"]
    y_col = ins["y_col"]

    if ins["chart_type"] == "line":
        viz_type = "echarts_timeseries_line"
        params = {
            "viz_type": "echarts_timeseries_line",
            "x_axis": x_col,
            "time_grain_sqla": None,
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": y_col},
                         "aggregate": "SUM", "label": y_col}],
            "groupby": [],
            "rich_tooltip": True,
            "show_legend": True,
        }
    elif ins["chart_type"] == "heatmap":
        viz_type = "heatmap"
        params = {
            "viz_type": "heatmap",
            "all_columns_x": x_col,
            "all_columns_y": y_col,
            "metric": {"expressionType": "SIMPLE", "column": {"column_name": "incident_count"},
                       "aggregate": "SUM", "label": "incident_count"},
            "linear_color_scheme": "oranges",
            "xscale_interval": 1,
            "yscale_interval": 1,
            "canvas_image_rendering": "auto",
            "normalize_across": "heatmap",
            "left_margin": "auto",
            "bottom_margin": "auto",
        }
    else:
        viz_type = "bar"
        params = {
            "viz_type": "bar",
            "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": y_col},
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
    """Generate Superset chart YAML."""
    viz_type, params_json = chart_params(ins)
    return f"""slice_name: {ins['title']}
viz_type: {viz_type}
params: '{params_json}'
cache_timeout: null
uuid: {ins['uuid']}
dataset_uuid: {ins['ds_uuid']}
version: 1.0.0
"""


def dashboard_yaml(loaded_insights):
    """Generate Superset dashboard YAML."""
    position = {}
    for i, ins in enumerate(loaded_insights):
        col = (i % 2) * 6
        row_key = f"ROW-{i // 2}"
        chart_id = f"CHART-{ins['uuid'][:8]}"
        if col == 0:
            position[row_key] = {
                "type": "ROW",
                "id": row_key,
                "children": [],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "parents": ["ROOT_ID", "GRID_ID"],
            }
        position[chart_id] = {
            "type": "CHART",
            "id": chart_id,
            "children": [],
            "meta": {
                "chartId": ins["uuid"],
                "width": 6,
                "height": 50,
                "sliceName": ins["title"],
            },
            "parents": ["ROOT_ID", "GRID_ID", row_key],
        }
        position[row_key]["children"].append(chart_id)

    chart_refs = "\n".join(
        f"- uuid: {ins['uuid']}" for ins in loaded_insights
    )

    return f"""dashboard_title: SF Police Incidents — Big Data Pipeline
description: EDA insights from SF PD Incident Reports 2018-Present
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
version: 1.0.0
charts:
{chart_refs}
"""


def write_export(loaded_insights):
    """Write Superset v1 export directory and ZIP file."""
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    root = f"dashboard_export_{ts}"

    os.makedirs(EXPORT_DIR, exist_ok=True)
    zip_path = os.path.join(EXPORT_DIR, ZIP_NAME)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # metadata
        zf.writestr(f"{root}/metadata.yaml",
                    f"version: 1.0.0\ntype: Dashboard\ntimestamp: '{ts}'\n")

        # database
        zf.writestr(f"{root}/databases/SF_Incidents_EDA.yaml", db_yaml())

        for ins in loaded_insights:
            # dataset
            zf.writestr(
                f"{root}/datasets/SF_Incidents_EDA/{ins['table']}.yaml",
                dataset_yaml(ins),
            )
            # chart
            safe_name = ins["title"].replace(" ", "_").replace("/", "-")
            zf.writestr(
                f"{root}/charts/{safe_name}.yaml",
                chart_yaml(ins),
            )

        # dashboard
        zf.writestr(
            f"{root}/dashboards/SF_Incidents_Dashboard.yaml",
            dashboard_yaml(loaded_insights),
        )

    print(f"\nSuperset export saved -> {zip_path}")
    print("Import via: Superset UI -> Dashboards -> (+) -> Import dashboard")
    return zip_path


def main():
    """Build SQLite DB from EDA CSVs and generate Superset export ZIP."""
    os.makedirs(EXPORT_DIR, exist_ok=True)

    print("=== Stage IV (Superset): Building EDA database ===")
    loaded = build_sqlite(INSIGHTS)

    if not loaded:
        print("\nNo EDA data found. Run stage2 first to generate output/eda/ CSVs.")
        print("Generating empty export structure for import template...")
        loaded = INSIGHTS  # generate YAML even without data

    print(f"\nLoaded {len(loaded)}/{len(INSIGHTS)} insights into {DB_PATH}")

    print("\n=== Generating Superset v1 export ZIP ===")
    zip_path = write_export(loaded)

    print("\nDone.")
    return zip_path


if __name__ == "__main__":
    main()
