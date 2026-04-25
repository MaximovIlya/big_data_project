"""Stage III: Multi-class classification of SF Police Incident Categories.

Models: Random Forest, LinearSVC, Naive Bayes — all via PySpark MLlib on YARN.
Hyperparameter tuning via CrossValidator + ParamGridBuilder.
Outputs: saved models, JSON metrics, sample predictions CSV.
"""

import argparse
import json
import os

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    LinearSVC,
    NaiveBayes,
    OneVsRest,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import (
    MinMaxScaler,
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    """Create Spark session with Hive support."""
    return (
        SparkSession.builder
        .appName("SF_Incidents_ML")
        .enableHiveSupport()
        .getOrCreate()
    )


def load_and_prepare(spark, hive_db, top_n):
    """Load data from Hive, engineer features, bucket rare categories."""
    spark.sql(f"USE {hive_db}")

    incidents_df = spark.sql("""
        SELECT
            incident_category,
            HOUR(incident_datetime)                     AS hour,
            DAYOFWEEK(incident_datetime)                AS day_of_week,
            MONTH(incident_datetime)                    AS month,
            incident_year                               AS year,
            latitude,
            longitude,
            COALESCE(police_district, 'Unknown')        AS police_district
        FROM incidents_parquet
        WHERE incident_category IS NOT NULL
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND incident_datetime IS NOT NULL
    """)

    # Keep only top-N categories; label the rest "Other"
    top_cats = (
        incidents_df.groupBy("incident_category")
        .count()
        .orderBy(F.desc("count"))
        .limit(top_n)
        .select("incident_category")
        .rdd.flatMap(lambda r: r)
        .collect()
    )
    top_cats_set = set(top_cats)

    incidents_df = incidents_df.withColumn(
        "label_raw",
        F.when(F.col("incident_category").isin(top_cats_set), F.col("incident_category"))
        .otherwise("Other"),
    )

    print(f"Label distribution (top {top_n} + Other):")
    incidents_df.groupBy("label_raw").count().orderBy(F.desc("count")).show(top_n + 2)

    return incidents_df


def build_feature_pipeline():
    """Build feature engineering pipeline stages (shared across models)."""
    district_indexer = StringIndexer(
        inputCol="police_district",
        outputCol="district_idx",
        handleInvalid="keep",
    )
    district_encoder = OneHotEncoder(
        inputCols=["district_idx"],
        outputCols=["district_vec"],
    )
    label_indexer = StringIndexer(
        inputCol="label_raw",
        outputCol="label",
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=["hour", "day_of_week", "month", "year",
                   "latitude", "longitude", "district_vec"],
        outputCol="features_raw",
        handleInvalid="skip",
    )
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=False,
    )
    return [district_indexer, district_encoder, label_indexer, assembler, scaler]


def evaluate_model(predictions, label_col="label", pred_col="prediction"):
    """Return accuracy and weighted F1 score."""
    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol=pred_col,
        metricName="accuracy",
    )
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol=pred_col,
        metricName="weightedFMeasure",
    )
    return evaluator_acc.evaluate(predictions), evaluator_f1.evaluate(predictions)


def train_random_forest(train_df, feature_stages, cv_folds):
    """Train Random Forest with 27-combination grid search + k-fold CV."""
    print("\n=== Model 1: Random Forest ===")

    rf_classifier = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        seed=42,
    )
    pipeline = Pipeline(stages=feature_stages + [rf_classifier])

    param_grid = (
        ParamGridBuilder()
        .addGrid(rf_classifier.numTrees, [50, 100, 200])      # 3
        .addGrid(rf_classifier.maxDepth, [5, 10, 15])          # 3
        .addGrid(rf_classifier.maxBins, [16, 32, 64])          # 3  → 27 total
        .build()
    )
    print(f"RF grid search: {len(param_grid)} hyperparameter combinations")

    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedFMeasure",
    )
    cross_validator = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=cv_folds,
        seed=42,
    )

    cv_model = cross_validator.fit(train_df)
    best_model = cv_model.bestModel

    print(f"Best RF params: numTrees={best_model.stages[-1].getNumTrees}, "
          f"maxDepth={best_model.stages[-1].getMaxDepth()}")

    return best_model


def train_linear_svc(train_df, feature_stages, cv_folds):
    """Train LinearSVC (one-vs-rest) with 27-combination grid search + k-fold CV."""
    print("\n=== Model 2: Linear SVC (One-vs-Rest) ===")

    svc = LinearSVC(labelCol="label", featuresCol="features", maxIter=100)
    ovr = OneVsRest(classifier=svc, labelCol="label", featuresCol="features")
    pipeline = Pipeline(stages=feature_stages + [ovr])

    param_grid = (
        ParamGridBuilder()
        .addGrid(svc.regParam, [0.01, 0.1, 1.0])   # 3
        .addGrid(svc.maxIter, [50, 100, 200])        # 3
        .addGrid(svc.tol, [1e-4, 1e-3, 1e-2])       # 3  → 27 total
        .build()
    )
    print(f"SVC grid search: {len(param_grid)} hyperparameter combinations")

    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedFMeasure",
    )
    cross_validator = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=cv_folds,
        seed=42,
    )

    cv_model = cross_validator.fit(train_df)
    best_model = cv_model.bestModel

    best_svc = best_model.stages[-1].models[0]  # first OvR sub-model
    print(f"Best SVC params: regParam={best_svc.getRegParam()}, "
          f"maxIter={best_svc.getMaxIter()}")

    return best_model


def train_naive_bayes(train_df, feature_stages, cv_folds):
    """Train Naive Bayes with smoothing grid search + k-fold CV.

    NaiveBayes requires non-negative features, so we use a MinMaxScaler
    stage instead of StandardScaler for this model.
    """
    print("\n=== Model 3: Naive Bayes ===")

    # Replace StandardScaler with MinMaxScaler for non-negative features
    nb_feature_stages = feature_stages[:-1]  # drop StandardScaler
    mm_scaler = MinMaxScaler(inputCol="features_raw", outputCol="features")

    nb_classifier = NaiveBayes(
        labelCol="label",
        featuresCol="features",
        modelType="multinomial",
    )
    pipeline = Pipeline(stages=nb_feature_stages + [mm_scaler, nb_classifier])

    param_grid = (
        ParamGridBuilder()
        .addGrid(nb_classifier.smoothing, [0.1, 0.5, 1.0, 1.5, 2.0])  # 5 values
        .build()
    )
    print(f"NB grid search: {len(param_grid)} hyperparameter combinations")

    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedFMeasure",
    )
    cross_validator = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=cv_folds,
        seed=42,
    )

    cv_model = cross_validator.fit(train_df)
    best_model = cv_model.bestModel

    print(f"Best NB params: smoothing={best_model.stages[-1].getSmoothing()}")

    return best_model


def save_sample_predictions(test_df, model, label_indexer_model,
                            predictions_dir, model_name, num_samples=5):
    """Save n sample predictions (predicted vs actual) to CSV."""
    preds = model.transform(test_df)

    # Reverse-map numeric label → string category
    labels = label_indexer_model.labels
    idx_to_label = F.udf(lambda i: labels[int(i)] if i is not None else "Unknown")

    sample = (
        preds
        .withColumn("actual_category", idx_to_label(F.col("label")))
        .withColumn("predicted_category", idx_to_label(F.col("prediction")))
        .select("actual_category", "predicted_category",
                "hour", "day_of_week", "police_district",
                "latitude", "longitude")
        .limit(num_samples)
    )
    sample.show(num_samples, truncate=False)

    out_path = os.path.join(predictions_dir, model_name)
    sample.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_path)
    print(f"Sample predictions saved to {out_path}")


def run_pipeline(args):
    """Full ML pipeline: load → feature engineer → train 3 models → evaluate."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.predictions_dir, exist_ok=True)

    # ── Data preparation ───────────────────────────────────────────────────
    incidents_df = load_and_prepare(spark, args.hive_db, args.top_n_categories)

    train_df, test_df = incidents_df.randomSplit([0.7, 0.3], seed=42)
    train_df.cache()
    test_df.cache()

    print(f"Train size: {train_df.count():,} | Test size: {test_df.count():,}")

    feature_stages = build_feature_pipeline()

    # ── Model 1: Random Forest ─────────────────────────────────────────────
    rf_model = train_random_forest(train_df, list(feature_stages), args.cv_folds)
    rf_preds = rf_model.transform(test_df)
    rf_acc, rf_f1 = evaluate_model(rf_preds)
    print(f"RF  →  Accuracy: {rf_acc:.4f} | Weighted F1: {rf_f1:.4f}")

    rf_model.write().overwrite().save(os.path.join(args.models_dir, "random_forest"))

    label_model = rf_model.stages[2]  # StringIndexer for labels
    save_sample_predictions(test_df, rf_model, label_model,
                            args.predictions_dir, "random_forest")

    # ── Model 2: Linear SVC ────────────────────────────────────────────────
    svc_model = train_linear_svc(train_df, list(feature_stages), args.cv_folds)
    svc_preds = svc_model.transform(test_df)
    svc_acc, svc_f1 = evaluate_model(svc_preds)
    print(f"SVC →  Accuracy: {svc_acc:.4f} | Weighted F1: {svc_f1:.4f}")

    svc_model.write().overwrite().save(os.path.join(args.models_dir, "linear_svc"))
    save_sample_predictions(test_df, svc_model, label_model,
                            args.predictions_dir, "linear_svc")

    # ── Model 3: Naive Bayes ───────────────────────────────────────────────
    nb_model = train_naive_bayes(train_df, list(feature_stages), args.cv_folds)
    nb_preds = nb_model.transform(test_df)
    nb_acc, nb_f1 = evaluate_model(nb_preds)
    print(f"NB  →  Accuracy: {nb_acc:.4f} | Weighted F1: {nb_f1:.4f}")

    nb_model.write().overwrite().save(os.path.join(args.models_dir, "naive_bayes"))
    save_sample_predictions(test_df, nb_model, label_model,
                            args.predictions_dir, "naive_bayes")

    # ── Save metrics summary ───────────────────────────────────────────────
    metrics = {
        "random_forest": {"accuracy": rf_acc, "weighted_f1": rf_f1},
        "linear_svc":    {"accuracy": svc_acc, "weighted_f1": svc_f1},
        "naive_bayes":   {"accuracy": nb_acc, "weighted_f1": nb_f1},
        "config": {
            "top_n_categories": args.top_n_categories,
            "cv_folds": args.cv_folds,
            "train_ratio": 0.7,
            "test_ratio": 0.3,
        },
    }

    summary_path = os.path.join(args.output_dir, "metrics_summary.json")
    with open(summary_path, "w", encoding="utf-8") as file_handle:
        json.dump(metrics, file_handle, indent=2)

    print(f"\nMetrics summary saved to {summary_path}")
    print("\n=== Final Results ===")
    print(f"{'Model':<20} {'Accuracy':>10} {'Weighted F1':>12}")
    print("-" * 44)
    for name, m in metrics.items():
        if name == "config":
            continue
        print(f"{name:<20} {m['accuracy']:>10.4f} {m['weighted_f1']:>12.4f}")

    spark.stop()


def main():
    """Parse arguments and run ML pipeline."""
    parser = argparse.ArgumentParser(description="SF Incidents ML Pipeline")
    parser.add_argument("--hive-db", default="sf_incidents_db")
    parser.add_argument("--output-dir", default="output/metrics")
    parser.add_argument("--models-dir", default="models")
    parser.add_argument("--predictions-dir", default="output/predictions")
    parser.add_argument("--top-n-categories", type=int, default=10)
    parser.add_argument("--cv-folds", type=int, default=5)
    args = parser.parse_args()

    run_pipeline(args)


if __name__ == "__main__":
    main()
