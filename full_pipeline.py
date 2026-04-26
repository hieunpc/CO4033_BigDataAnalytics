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
from brfss_pipeline.pipeline.full_pipeline import run_pipeline


CONFIG_PATH = Path("pipeline_config.json")


def setup_logging() -> None:
    log_dir = Path("outputs/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "pipeline.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
    )


def load_config(config_path: Path) -> dict:
    defaults = {
        "input_csv": str(RAW_DATA_PATH),
        "selected_csv": str(PROCESSED_DATA_PATH),
        "use_kafka": False,
        "kafka_topic": DEFAULT_TOPIC,
        "kafka_bootstrap_servers": DEFAULT_BOOTSTRAP_SERVERS,
        "metrics_path": str(DEFAULT_METRICS_PATH),
        "spark_metrics_path": "outputs/metrics/spark_metrics.json",
        "plot_dir": "outputs/metrics/plots",
        "runtime_metadata_path": "outputs/metrics/pipeline_runtime.json",
    }

    if not config_path.exists():
        return defaults

    loaded = json.loads(config_path.read_text(encoding="utf-8"))
    defaults.update(loaded)
    return defaults


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)
    config = load_config(CONFIG_PATH)
    logger.info("Loaded config from %s", CONFIG_PATH)
    payload = run_pipeline(
        input_csv=config["input_csv"],
        selected_csv=config["selected_csv"],
        use_kafka=config["use_kafka"],
        kafka_topic=config["kafka_topic"],
        kafka_bootstrap_servers=config["kafka_bootstrap_servers"],
        metrics_path=config["metrics_path"],
        spark_metrics_path=config["spark_metrics_path"],
        plot_dir=config["plot_dir"],
        runtime_metadata_path=config["runtime_metadata_path"],
    )
    logger.info("Pipeline run finished with %d model results", len(payload))
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
