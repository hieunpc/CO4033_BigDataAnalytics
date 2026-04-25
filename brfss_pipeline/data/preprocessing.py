from __future__ import annotations

import pandas as pd

from brfss_pipeline.config import TARGET_COLUMN


def clean_brfss_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()

    object_columns = list(cleaned.select_dtypes(include=["object"]).columns)
    if object_columns:
        cleaned = cleaned.drop(columns=object_columns)

    if TARGET_COLUMN not in cleaned.columns:
        raise ValueError(f"Target column {TARGET_COLUMN!r} is missing from the dataframe")

    cleaned[TARGET_COLUMN] = cleaned[TARGET_COLUMN].replace({2: 0})
    cleaned = cleaned[~cleaned[TARGET_COLUMN].isin([7, 9])]
    cleaned = cleaned.dropna(subset=[TARGET_COLUMN])

    constant_columns = [column for column in cleaned.columns if cleaned[column].nunique(dropna=False) == 1]
    if constant_columns:
        cleaned = cleaned.drop(columns=constant_columns)

    numeric_columns = list(cleaned.select_dtypes(include=["number"]).columns)
    if numeric_columns:
        cleaned[numeric_columns] = cleaned[numeric_columns].apply(pd.to_numeric, errors="coerce")
        cleaned[numeric_columns] = cleaned[numeric_columns].fillna(cleaned[numeric_columns].median())

    cleaned[TARGET_COLUMN] = cleaned[TARGET_COLUMN].round().astype(int)
    return cleaned
