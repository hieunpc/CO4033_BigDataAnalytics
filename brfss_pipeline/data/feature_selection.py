from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import pandas as pd
from sklearn.feature_selection import mutual_info_classif

from brfss_pipeline.config import PROCESSED_DATA_PATH, RAW_DATA_PATH, SELECTED_COLUMNS, TARGET_COLUMN


def run_feature_selection(
    input_path: str | Path = RAW_DATA_PATH,
    output_path: str | Path = PROCESSED_DATA_PATH,
) -> Tuple[pd.DataFrame, List[Tuple[str, float]]]:
    df = pd.read_csv(input_path)
    available_columns = [column for column in SELECTED_COLUMNS if column in df.columns]
    if TARGET_COLUMN not in available_columns:
        raise ValueError(f"Missing target column {TARGET_COLUMN}")
    cleaned = df[available_columns].copy()

    object_columns = cleaned.select_dtypes(include=["object"]).columns
    if len(object_columns) > 0:
        cleaned = cleaned.drop(columns=object_columns)

    cleaned[TARGET_COLUMN] = cleaned[TARGET_COLUMN].replace(2, 0)
    cleaned = cleaned[~cleaned[TARGET_COLUMN].isin([7, 9])]
    cleaned = cleaned.dropna(subset=[TARGET_COLUMN])

    total_rows = len(cleaned)
    missing_stats = (
        cleaned.isna().sum()
        .to_frame(name="missing")
        .assign(missing_pct=lambda x: (x["missing"] / total_rows) * 100)
        .sort_values(by="missing", ascending=False)
        .query("missing > 0")
    )
    to_drop = missing_stats[missing_stats["missing_pct"] > 40].index
    cleaned = cleaned.drop(columns=to_drop)

    constant_columns = [col for col in cleaned.columns if cleaned[col].nunique(dropna=False) == 1]
    if constant_columns:
        cleaned = cleaned.drop(columns=constant_columns)

    numeric_columns = list(cleaned.select_dtypes(include=["number"]).columns)
    cleaned[numeric_columns] = cleaned[numeric_columns].apply(pd.to_numeric, errors="coerce")
    cleaned[numeric_columns] = cleaned[numeric_columns].fillna(cleaned[numeric_columns].median())

    X = cleaned.drop(columns=[TARGET_COLUMN])
    y = cleaned[TARGET_COLUMN]
    ig_scores = mutual_info_classif(X, y, discrete_features="auto")
    sorted_ig = sorted(dict(zip(X.columns, ig_scores)).items(), key=lambda x: x[1], reverse=True)

    cleaned = cleaned[[column for column in SELECTED_COLUMNS if column in cleaned.columns]]
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    cleaned.to_csv(output_path, index=False)
    return cleaned, sorted_ig
