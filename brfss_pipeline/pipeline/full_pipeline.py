from __future__ import annotations

import json
import logging
from pathlib import Path

from brfss_pipeline.config import (
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_METRICS_PATH,
    DEFAULT_TOPIC,
    PROCESSED_DATA_PATH,
    RAW_DATA_PATH,
)
from brfss_pipeline.data.feature_selection import run_feature_selection
from brfss_pipeline.streaming.spark_io import build_spark_session, save_metric_bar_charts, train_spark_models
from brfss_pipeline.streaming.kafka_io import produce_to_kafka


logger = logging.getLogger(__name__)


def run_pipeline(
    input_csv: str | Path = RAW_DATA_PATH,
    selected_csv: str | Path = PROCESSED_DATA_PATH,
    use_kafka: bool = False,
    kafka_topic: str = DEFAULT_TOPIC,
    kafka_bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    metrics_path: str | Path = DEFAULT_METRICS_PATH,
    spark_metrics_path: str | Path = "outputs/metrics/spark_metrics.json",
    plot_dir: str | Path = "outputs/metrics/plots",
):
    logger.info("Pipeline started")
    logger.info("Input CSV: %s", input_csv)
    logger.info("Selected CSV output: %s", selected_csv)
    logger.info("Use Kafka: %s", use_kafka)

    logger.info("Step 1/4: Feature selection")
    run_feature_selection(input_csv, selected_csv)
    logger.info("Feature selection completed")

    if use_kafka:
        logger.info("Step 2/4: Publish selected data to Kafka (%s)", kafka_topic)
        produce_to_kafka(
            csv_path=selected_csv,
            bootstrap_servers=kafka_bootstrap_servers,
            topic=kafka_topic,
        )
        logger.info("Kafka publish completed")
    else:
        logger.info("Step 2/4: Kafka publish skipped")

    logger.info("Step 3/4: Spark train on selected CSV")
    spark = build_spark_session("BRFSSSparkTrain")
    try:
        results = train_spark_models(spark, selected_csv)
    finally:
        spark.stop()
    logger.info("Spark training completed with %d models", len(results))

    plot_output_dir = Path(plot_dir)
    chart_paths = save_metric_bar_charts(results, plot_output_dir)
    logger.info("Saved %d metric charts to: %s", len(chart_paths), plot_output_dir)

    payload = results
    Path(metrics_path).parent.mkdir(parents=True, exist_ok=True)
    Path(metrics_path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("Saved metrics: %s", metrics_path)

    best_summary = sorted(results, key=lambda item: (item["f1"], item["roc_auc"], item["accuracy"]), reverse=True)[0]
    best_model_path = Path(metrics_path).with_name("best_model.json")
    best_model_path.write_text(json.dumps(best_summary, indent=2), encoding="utf-8")
    logger.info("Saved best model summary: %s", best_model_path)

    spark_metrics_path = Path(spark_metrics_path)
    spark_metrics_path.parent.mkdir(parents=True, exist_ok=True)
    spark_metrics_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    logger.info("Saved Spark metrics: %s", spark_metrics_path)

    logger.info("Step 4/4: Export metric charts")
    logger.info("Pipeline finished successfully")
    return payload

