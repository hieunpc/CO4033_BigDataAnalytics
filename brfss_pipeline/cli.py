from __future__ import annotations

import argparse
import json
from pathlib import Path

from brfss_pipeline.config import (
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_METRICS_PATH,
    DEFAULT_TOPIC,
    PROCESSED_DATA_PATH,
    RAW_DATA_PATH,
)
from brfss_pipeline.data.feature_selection import run_feature_selection
from brfss_pipeline.pipeline.full_pipeline import run_pipeline
from brfss_pipeline.streaming.kafka_io import produce_to_kafka
from brfss_pipeline.streaming.spark_io import build_spark_session, train_spark_models


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BRFSS pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fs_parser = subparsers.add_parser("feature-select", help="Run feature selection and write selected dataset")
    fs_parser.add_argument("--input-path", default=str(RAW_DATA_PATH))
    fs_parser.add_argument("--output-path", default=str(PROCESSED_DATA_PATH))

    k_parser = subparsers.add_parser("kafka-produce", help="Publish selected records to Kafka")
    k_parser.add_argument("--csv-path", default=str(PROCESSED_DATA_PATH))
    k_parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    k_parser.add_argument("--topic", default=DEFAULT_TOPIC)
    k_parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)

    t_parser = subparsers.add_parser("spark-train", help="Train Spark ML models from parquet or CSV")
    t_parser.add_argument("--input-path", default=str(PROCESSED_DATA_PATH))
    t_parser.add_argument("--output-path", default="outputs/metrics/spark_metrics.json")
    t_parser.add_argument("--plot-dir", default="outputs/metrics/plots")

    r_parser = subparsers.add_parser("run-all", help="Run full pipeline (feature select -> optional Kafka -> Spark train)")
    r_parser.add_argument("--input-csv", default=str(RAW_DATA_PATH))
    r_parser.add_argument("--selected-csv", default=str(PROCESSED_DATA_PATH))
    r_parser.add_argument("--use-kafka", action="store_true")
    r_parser.add_argument("--kafka-topic", default=DEFAULT_TOPIC)
    r_parser.add_argument("--kafka-bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    r_parser.add_argument("--metrics-path", default=str(DEFAULT_METRICS_PATH))
    r_parser.add_argument("--spark-metrics-path", default="outputs/metrics/spark_metrics.json")
    r_parser.add_argument("--plot-dir", default="outputs/metrics/plots")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.command == "feature-select":
        _, sorted_ig = run_feature_selection(args.input_path, args.output_path)
        print(f"Saved selected dataset: {args.output_path}")
        print("Top 10 features by mutual information:")
        for feature, score in sorted_ig[:10]:
            print(f"{feature}: {score:.4f}")
        return

    if args.command == "kafka-produce":
        produce_to_kafka(
            csv_path=args.csv_path,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            chunk_size=args.chunk_size,
        )
        print("Kafka publish completed")
        return

    if args.command == "spark-train":
        spark = build_spark_session("BRFSSSparkTrain")
        metrics = train_spark_models(spark, args.input_path)
        output_path = Path(args.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        plot_dir = Path(args.plot_dir)
        plot_dir.mkdir(parents=True, exist_ok=True)
        print(json.dumps(metrics, indent=2))
        return

    if args.command == "run-all":
        payload = run_pipeline(
            input_csv=args.input_csv,
            selected_csv=args.selected_csv,
            use_kafka=args.use_kafka,
            kafka_topic=args.kafka_topic,
            kafka_bootstrap_servers=args.kafka_bootstrap_servers,
            metrics_path=args.metrics_path,
            spark_metrics_path=args.spark_metrics_path,
            plot_dir=args.plot_dir,
        )
        print(json.dumps(payload, indent=2))
        return

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
