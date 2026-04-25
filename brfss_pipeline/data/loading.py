from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable

import pandas as pd

from brfss_pipeline.config import (
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_TOPIC,
    PROCESSED_DATA_PATH,
    SELECTED_COLUMNS,
    TARGET_COLUMN,
)
from brfss_pipeline.data.preprocessing import clean_brfss_dataframe


def load_brfss_dataframe(csv_path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    available_columns = [column for column in SELECTED_COLUMNS if column in df.columns]
    if TARGET_COLUMN not in available_columns:
        raise ValueError(f"Target column {TARGET_COLUMN!r} was not found in {csv_path}")
    return df[available_columns].copy()


def iter_brfss_records(csv_path: str | Path, chunk_size: int = DEFAULT_CHUNK_SIZE) -> Iterable[Dict[str, object]]:
    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        available_columns = [column for column in SELECTED_COLUMNS if column in chunk.columns]
        chunk = chunk[available_columns].copy()
        chunk = clean_brfss_dataframe(chunk)
        for record in chunk.to_dict(orient="records"):
            yield record


def load_brfss_for_notebooks(
    source_csv: str | Path = PROCESSED_DATA_PATH,
    use_kafka: bool = False,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_TOPIC,
) -> pd.DataFrame:
    if use_kafka:
        try:
            from brfss_pipeline.streaming.spark_io import build_spark_session, spark_row_schema
            import importlib

            spark = build_spark_session(app_name="BRFSSNotebookLoader")
            F = importlib.import_module("pyspark.sql.functions")

            schema = spark_row_schema([column for column in SELECTED_COLUMNS if column != TARGET_COLUMN])
            kafka_df = (
                spark.read.format("kafka")
                .option("kafka.bootstrap.servers", bootstrap_servers)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
            )
            parsed_df = (
                kafka_df.select(F.col("value").cast("string").alias("json_value"))
                .select(F.from_json(F.col("json_value"), schema).alias("data"))
                .select("data.*")
            )
            pandas_df = parsed_df.toPandas()
            spark.stop()
            return clean_brfss_dataframe(pandas_df)
        except Exception:
            pass

    source = Path(source_csv)
    if source.exists():
        return clean_brfss_dataframe(load_brfss_dataframe(source))

    raise FileNotFoundError(f"Could not load BRFSS data from Kafka or {source_csv}")
