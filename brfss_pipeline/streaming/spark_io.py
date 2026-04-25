from __future__ import annotations

import importlib
import os
import re
from pathlib import Path
from typing import Dict, List, Sequence

import numpy as np

from brfss_pipeline.config import DEFAULT_TOPIC, SELECTED_COLUMNS, TARGET_COLUMN


def _spark_imports():
    pyspark_sql = importlib.import_module("pyspark.sql")
    pyspark_sql_functions = importlib.import_module("pyspark.sql.functions")
    pyspark_sql_types = importlib.import_module("pyspark.sql.types")
    pyspark_ml = importlib.import_module("pyspark.ml")
    pyspark_ml_classification = importlib.import_module("pyspark.ml.classification")
    pyspark_ml_evaluation = importlib.import_module("pyspark.ml.evaluation")
    pyspark_ml_feature = importlib.import_module("pyspark.ml.feature")

    return {
        "F": pyspark_sql_functions,
        "T": pyspark_sql_types,
        "SparkSession": pyspark_sql.SparkSession,
        "Pipeline": pyspark_ml.Pipeline,
        "DecisionTreeClassifier": pyspark_ml_classification.DecisionTreeClassifier,
        "LinearSVC": pyspark_ml_classification.LinearSVC,
        "LogisticRegression": pyspark_ml_classification.LogisticRegression,
        "NaiveBayes": pyspark_ml_classification.NaiveBayes,
        "RandomForestClassifier": pyspark_ml_classification.RandomForestClassifier,
        "BinaryClassificationEvaluator": pyspark_ml_evaluation.BinaryClassificationEvaluator,
        "MulticlassClassificationEvaluator": pyspark_ml_evaluation.MulticlassClassificationEvaluator,
        "Imputer": pyspark_ml_feature.Imputer,
        "VectorAssembler": pyspark_ml_feature.VectorAssembler,
    }


def _detect_scala_binary_version() -> str:
    pyspark_module = importlib.import_module("pyspark")
    jar_dir = Path(pyspark_module.__file__).resolve().parent / "jars"
    candidates = list(jar_dir.glob("spark-sql_*.jar"))
    for jar_path in candidates:
        match = re.search(r"spark-sql_(\d+\.\d+)-", jar_path.name)
        if match:
            return match.group(1)
    return "2.13"


def _ensure_windows_hadoop_home() -> Path | None:
    if os.name != "nt":
        return None

    project_hadoop = Path(".hadoop").resolve()
    winutils_path = project_hadoop / "bin" / "winutils.exe"
    if not winutils_path.exists():
        return None

    os.environ.setdefault("HADOOP_HOME", str(project_hadoop))
    os.environ.setdefault("hadoop.home.dir", str(project_hadoop))
    return project_hadoop


def build_spark_session(
    app_name: str = "BRFSSKafkaSparkML",
    include_kafka_package: bool = False,
    kafka_package: str | None = None,
):
    spark_version = importlib.import_module("pyspark").__version__
    spark_base_version = ".".join(spark_version.split(".")[:3])
    scala_binary = _detect_scala_binary_version()
    ivy_dir = str((Path(".spark-ivy").resolve()))

    if include_kafka_package and kafka_package is None:
        kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_{scala_binary}:{spark_base_version}"

    spark_imports = _spark_imports()
    hadoop_home = _ensure_windows_hadoop_home()
    builder = (
        spark_imports["SparkSession"].builder.appName(app_name)
        .master("local[4]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.local.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
    )
    if hadoop_home is not None:
        builder = builder.config("spark.hadoop.hadoop.home.dir", str(hadoop_home))
    if include_kafka_package and kafka_package:
        builder = builder.config("spark.jars.packages", kafka_package)
    return builder.getOrCreate()


def spark_row_schema(columns: Sequence[str]):
    spark_imports = _spark_imports()
    T = spark_imports["T"]
    return T.StructType([T.StructField(column, T.DoubleType(), True) for column in columns])


def kafka_stream_dataframe(spark, bootstrap_servers: str, topic: str = DEFAULT_TOPIC):
    spark_imports = _spark_imports()
    F = spark_imports["F"]

    schema = spark_row_schema(SELECTED_COLUMNS)
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    return (
        raw_stream.select(F.col("value").cast("string").alias("json_value"))
        .select(F.from_json(F.col("json_value"), schema).alias("data"))
        .select("data.*")
    )


def write_kafka_stream_to_parquet(
    spark,
    bootstrap_servers: str,
    topic: str,
    output_path: str | Path,
    checkpoint_path: str | Path,
    trigger_once: bool = False,
    timeout_seconds: int | None = None,
) -> None:
    spark_imports = _spark_imports()
    F = spark_imports["F"]

    # On Windows, structured streaming checkpoints can hit native Hadoop issues.
    # For one-shot ingest, use Kafka batch read with Spark and write parquet directly.
    if trigger_once:
        schema = spark_row_schema(SELECTED_COLUMNS)
        batch_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
            .select(F.col("value").cast("string").alias("json_value"))
            .select(F.from_json(F.col("json_value"), schema).alias("data"))
            .select("data.*")
        )
        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            batch_df.write.mode("overwrite").parquet(str(output_dir))
        except Exception:
            # Fallback for Windows Hadoop native I/O issues while still using Spark for Kafka ingest.
            pandas_df = batch_df.toPandas()
            pandas_df.to_parquet(output_dir / "kafka_ingest.parquet", index=False)
        return

    stream_df = kafka_stream_dataframe(spark, bootstrap_servers, topic)
    writer = (
        stream_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", str(output_path))
        .option("checkpointLocation", str(checkpoint_path))
    )
    query = writer.start()
    if timeout_seconds is None:
        query.awaitTermination()
        return

    finished = query.awaitTermination(int(timeout_seconds * 1000))
    if not finished:
        query.stop()
        raise TimeoutError(
            f"Spark ingest did not finish within {timeout_seconds} seconds. "
            "Increase timeout_seconds or verify Kafka has incoming data."
        )


def load_training_frame(spark, input_path: str | Path):
    spark_imports = _spark_imports()
    F = spark_imports["F"]

    input_path = str(input_path)
    if input_path.lower().endswith(".csv"):
        df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    else:
        df = spark.read.parquet(input_path)

    available_columns = [column for column in SELECTED_COLUMNS if column in df.columns]
    if TARGET_COLUMN not in available_columns:
        raise ValueError(f"Target column {TARGET_COLUMN!r} was not found in {input_path}")

    df = df.select(*available_columns)
    for column in available_columns:
        df = df.withColumn(column, df[column].cast("double"))

    df = df.withColumn(
        TARGET_COLUMN,
        F.when(F.col(TARGET_COLUMN) == 2, F.lit(0.0))
        .when(F.col(TARGET_COLUMN).isin(7, 9), F.lit(None))
        .otherwise(F.col(TARGET_COLUMN)),
    )
    return df.filter(F.col(TARGET_COLUMN).isNotNull())


def train_spark_models(spark, input_path: str | Path) -> List[Dict[str, float]]:
    spark_imports = _spark_imports()
    Pipeline = spark_imports["Pipeline"]
    Imputer = spark_imports["Imputer"]
    VectorAssembler = spark_imports["VectorAssembler"]
    LogisticRegression = spark_imports["LogisticRegression"]
    RandomForestClassifier = spark_imports["RandomForestClassifier"]
    DecisionTreeClassifier = spark_imports["DecisionTreeClassifier"]
    BinaryClassificationEvaluator = spark_imports["BinaryClassificationEvaluator"]
    MulticlassClassificationEvaluator = spark_imports["MulticlassClassificationEvaluator"]

    df = load_training_frame(spark, input_path).withColumnRenamed(TARGET_COLUMN, "label")
    feature_columns = [column for column in df.columns if column != "label"]
    if not feature_columns:
        raise ValueError("No feature columns were available after cleaning the dataset")

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df = train_df.cache()
    test_df = test_df.cache()
    train_df.count()
    test_df.count()
    imputed_columns = [f"{column}_imputed" for column in feature_columns]
    imputer = Imputer(inputCols=feature_columns, outputCols=imputed_columns)
    assembler = VectorAssembler(inputCols=imputed_columns, outputCol="features", handleInvalid="keep")

    estimators = {
        "logistic_regression": LogisticRegression(featuresCol="features", labelCol="label", maxIter=40),
        "random_forest": RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=40, maxDepth=8, seed=42),
        "decision_tree": DecisionTreeClassifier(featuresCol="features", labelCol="label", maxDepth=8, seed=42),
    }

    roc_evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    accuracy_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    f1_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

    results: List[Dict[str, float]] = []
    for model_name, estimator in estimators.items():
        pipeline = Pipeline(stages=[imputer, assembler, estimator])
        model = pipeline.fit(train_df)
        predictions = model.transform(test_df).cache()

        metrics = {
            "model": model_name,
            "accuracy": float(accuracy_evaluator.evaluate(predictions)),
            "f1": float(f1_evaluator.evaluate(predictions)),
            "roc_auc": float("nan"),
        }
        metrics["roc_auc"] = float(roc_evaluator.evaluate(predictions))

        results.append(metrics)
        predictions.unpersist()

    train_df.unpersist()
    test_df.unpersist()
    return sorted(results, key=lambda item: (item["f1"], item["roc_auc"], item["accuracy"]), reverse=True)


def save_metric_bar_charts(results: List[Dict[str, float]], output_dir: str | Path) -> List[str]:
    import matplotlib.pyplot as plt

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    chart_paths: List[str] = []
    metric_names = ["accuracy", "f1", "roc_auc"]
    model_names = [result["model"] for result in results]

    for metric_name in metric_names:
        values = [float(result.get(metric_name, float("nan"))) for result in results]
        safe_values = [0.0 if np.isnan(value) else value for value in values]
        plt.figure(figsize=(8, 4))
        plt.bar(model_names, safe_values, color="#1d3557")
        plt.ylim(0, 1)
        plt.title(f"Spark Model Comparison: {metric_name.upper()}")
        plt.ylabel(metric_name)
        for index, value in enumerate(values):
            label = "nan" if np.isnan(value) else f"{value:.3f}"
            plt.text(index, safe_values[index] + 0.01, label, ha="center", va="bottom", fontsize=8)
        plt.tight_layout()
        chart_path = output_dir / f"spark_metric_{metric_name}.png"
        plt.savefig(chart_path, dpi=150)
        plt.close()
        chart_paths.append(str(chart_path))

    return chart_paths
