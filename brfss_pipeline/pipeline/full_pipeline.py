from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
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
    runtime_metadata_path: str | Path = "outputs/metrics/pipeline_runtime.json",
):
    started_at = datetime.now(timezone.utc)
    start_time = time.perf_counter()
    step_durations: dict[str, float] = {}
    results: list[dict[str, float]] = []
    best_summary: dict[str, float] | None = None
    chart_paths: list[str] = []
    status = "success"
    error_message: str | None = None

    logger.info("Pipeline started")
    logger.info("Input CSV: %s", input_csv)
    logger.info("Selected CSV output: %s", selected_csv)
    logger.info("Use Kafka: %s", use_kafka)
    logger.info("Runtime metadata output: %s", runtime_metadata_path)

    try:
        logger.info("Step 1/4: Feature selection")
        step_started = time.perf_counter()
        run_feature_selection(input_csv, selected_csv)
        step_durations["feature_selection_seconds"] = round(time.perf_counter() - step_started, 4)
        logger.info("Feature selection completed")

        if use_kafka:
            logger.info("Step 2/4: Publish selected data to Kafka (%s)", kafka_topic)
            step_started = time.perf_counter()
            produce_to_kafka(
                csv_path=selected_csv,
                bootstrap_servers=kafka_bootstrap_servers,
                topic=kafka_topic,
            )
            step_durations["kafka_publish_seconds"] = round(time.perf_counter() - step_started, 4)
            logger.info("Kafka publish completed")
        else:
            step_durations["kafka_publish_seconds"] = 0.0
            logger.info("Step 2/4: Kafka publish skipped")

        logger.info("Step 3/4: Spark train on selected CSV")
        step_started = time.perf_counter()
        spark = build_spark_session("BRFSSSparkTrain")
        try:
            results = train_spark_models(spark, selected_csv)
        finally:
            spark.stop()
        step_durations["spark_train_seconds"] = round(time.perf_counter() - step_started, 4)
        logger.info("Spark training completed with %d models", len(results))

        plot_output_dir = Path(plot_dir)
        step_started = time.perf_counter()
        chart_paths = save_metric_bar_charts(results, plot_output_dir)
        step_durations["plot_export_seconds"] = round(time.perf_counter() - step_started, 4)
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
    except Exception as exc:
        status = "failed"
        error_message = str(exc)
        logger.exception("Pipeline failed")
        raise
    finally:
        finished_at = datetime.now(timezone.utc)
        duration_seconds = round(time.perf_counter() - start_time, 4)
        runtime_metadata = {
            "status": status,
            "started_at_utc": started_at.isoformat(),
            "finished_at_utc": finished_at.isoformat(),
            "duration_seconds": duration_seconds,
            "input_csv": str(input_csv),
            "selected_csv": str(selected_csv),
            "use_kafka": use_kafka,
            "kafka_topic": kafka_topic,
            "kafka_bootstrap_servers": kafka_bootstrap_servers,
            "metrics_path": str(metrics_path),
            "spark_metrics_path": str(spark_metrics_path),
            "plot_dir": str(plot_dir),
            "model_count": len(results),
            "models": [result.get("model", "") for result in results],
            "best_model": best_summary,
            "step_durations_seconds": step_durations,
            "chart_paths": chart_paths,
            "error_message": error_message,
        }
        runtime_metadata_path = Path(runtime_metadata_path)
        runtime_metadata_path.parent.mkdir(parents=True, exist_ok=True)
        runtime_metadata_path.write_text(json.dumps(runtime_metadata, indent=2), encoding="utf-8")
        logger.info("Saved runtime metadata: %s", runtime_metadata_path)

