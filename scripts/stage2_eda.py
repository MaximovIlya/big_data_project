"""Stage II: EDA of SF Police Incidents using PySpark SQL.

Produces 8 insights saved as CSV directories to output/eda/.
"""

import argparse
import os

from pyspark.sql import SparkSession


def create_spark_session():
    """Create a Spark session with Hive support."""
    return (
        SparkSession.builder
        .appName("SF_Incidents_EDA")
        .enableHiveSupport()
        .getOrCreate()
    )


def _save(df, output_dir, name):
    """Save a DataFrame as a single CSV part file."""
    path = os.path.join(output_dir, name)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)


def run_eda(spark, hive_db, output_dir):
    """Run all EDA queries and save results to output_dir."""
    spark.sql(f"USE {hive_db}")
    os.makedirs(output_dir, exist_ok=True)

    table = "incidents_parquet"

    # ── Insight 1: Top 10 incident categories ─────────────────────────────
    print("Insight 1: Top 10 incident categories")
    insight1 = spark.sql(f"""
        SELECT
            incident_category,
            COUNT(*) AS incident_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM {table}
        WHERE incident_category IS NOT NULL
        GROUP BY incident_category
        ORDER BY incident_count DESC
        LIMIT 10
    """)
    insight1.show(truncate=False)
    _save(insight1, output_dir, "insight1_top_categories")

    # ── Insight 2: Incidents by hour of day ───────────────────────────────
    print("Insight 2: Incidents by hour of day")
    insight2 = spark.sql(f"""
        SELECT
            HOUR(incident_datetime) AS hour_of_day,
            COUNT(*) AS incident_count
        FROM {table}
        WHERE incident_datetime IS NOT NULL
        GROUP BY HOUR(incident_datetime)
        ORDER BY hour_of_day
    """)
    insight2.show(24, truncate=False)
    _save(insight2, output_dir, "insight2_by_hour")

    # ── Insight 3: Incidents by day of week ───────────────────────────────
    print("Insight 3: Incidents by day of week")
    insight3 = spark.sql(f"""
        SELECT
            incident_day_of_week,
            COUNT(*) AS incident_count
        FROM {table}
        WHERE incident_day_of_week IS NOT NULL
        GROUP BY incident_day_of_week
        ORDER BY incident_count DESC
    """)
    insight3.show(truncate=False)
    _save(insight3, output_dir, "insight3_by_day_of_week")

    # ── Insight 4: Incidents by police district ───────────────────────────
    print("Insight 4: Incidents by police district")
    insight4 = spark.sql(f"""
        SELECT
            COALESCE(police_district, 'Unknown') AS police_district,
            COUNT(*) AS incident_count,
            ROUND(AVG(latitude), 4)  AS avg_lat,
            ROUND(AVG(longitude), 4) AS avg_lon
        FROM {table}
        GROUP BY COALESCE(police_district, 'Unknown')
        ORDER BY incident_count DESC
    """)
    insight4.show(truncate=False)
    _save(insight4, output_dir, "insight4_by_district")

    # ── Insight 5: Monthly trend over years ───────────────────────────────
    print("Insight 5: Monthly trend over years")
    insight5 = spark.sql(f"""
        SELECT
            incident_year,
            MONTH(incident_datetime) AS month,
            COUNT(*) AS incident_count
        FROM {table}
        WHERE incident_datetime IS NOT NULL
          AND incident_year >= 2018
        GROUP BY incident_year, MONTH(incident_datetime)
        ORDER BY incident_year, month
    """)
    insight5.show(50, truncate=False)
    _save(insight5, output_dir, "insight5_monthly_trend")

    # ── Insight 6: Resolution rate by top 10 categories ───────────────────
    print("Insight 6: Resolution rate by category")
    insight6 = spark.sql(f"""
        WITH top_cats AS (
            SELECT incident_category
            FROM {table}
            WHERE incident_category IS NOT NULL
            GROUP BY incident_category
            ORDER BY COUNT(*) DESC
            LIMIT 10
        )
        SELECT
            t.incident_category,
            COUNT(*) AS total,
            SUM(CASE WHEN i.resolution != 'Open or Active' THEN 1 ELSE 0 END) AS resolved,
            ROUND(
                SUM(CASE WHEN i.resolution != 'Open or Active' THEN 1 ELSE 0 END)
                * 100.0 / COUNT(*), 2
            ) AS resolution_rate_pct
        FROM {table} i
        JOIN top_cats t ON i.incident_category = t.incident_category
        GROUP BY t.incident_category
        ORDER BY resolution_rate_pct DESC
    """)
    insight6.show(truncate=False)
    _save(insight6, output_dir, "insight6_resolution_rate")

    # ── Insight 7: Top 15 neighborhoods by incident count ─────────────────
    print("Insight 7: Top neighborhoods by incident count")
    insight7 = spark.sql(f"""
        SELECT
            COALESCE(analysis_neighborhood, 'Unknown') AS neighborhood,
            COUNT(*) AS incident_count
        FROM {table}
        GROUP BY COALESCE(analysis_neighborhood, 'Unknown')
        ORDER BY incident_count DESC
        LIMIT 15
    """)
    insight7.show(truncate=False)
    _save(insight7, output_dir, "insight7_top_neighborhoods")

    # ── Insight 8: Heatmap — hour × day of week ───────────────────────────
    print("Insight 8: Incident heatmap hour x day_of_week")
    insight8 = spark.sql(f"""
        SELECT
            HOUR(incident_datetime)  AS hour_of_day,
            incident_day_of_week,
            COUNT(*)                 AS incident_count
        FROM {table}
        WHERE incident_datetime IS NOT NULL
          AND incident_day_of_week IS NOT NULL
        GROUP BY HOUR(incident_datetime), incident_day_of_week
        ORDER BY hour_of_day, incident_day_of_week
    """)
    insight8.show(50, truncate=False)
    _save(insight8, output_dir, "insight8_hour_day_heatmap")

    # ── Summary stats ─────────────────────────────────────────────────────
    total = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").collect()[0]["n"]
    n_cats = spark.sql(
        f"SELECT COUNT(DISTINCT incident_category) AS n FROM {table}"
    ).collect()[0]["n"]
    n_dist = spark.sql(
        f"SELECT COUNT(DISTINCT police_district) AS n FROM {table}"
    ).collect()[0]["n"]

    summary = spark.createDataFrame([{
        "total_incidents": total,
        "distinct_categories": n_cats,
        "distinct_districts": n_dist,
    }])
    _save(summary, output_dir, "summary_stats")

    print(f"\nDataset: {total:,} incidents | {n_cats} categories | {n_dist} districts")
    print(f"EDA complete — results in {output_dir}/")


def main():
    """Parse arguments and run EDA pipeline."""
    parser = argparse.ArgumentParser(description="SF Incidents EDA")
    parser.add_argument("--hive-db", default="sf_incidents_db")
    parser.add_argument("--output-dir", default="output/eda")
    args = parser.parse_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    run_eda(spark, args.hive_db, args.output_dir)
    spark.stop()


if __name__ == "__main__":
    main()
