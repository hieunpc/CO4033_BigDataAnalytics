from __future__ import annotations

from pathlib import Path
from typing import List

TARGET_COLUMN = "CVDINFR4"
DEFAULT_TOPIC = "brfss_health"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_CHUNK_SIZE = 2000

RAW_DATA_PATH = Path("data/raw/BRFSS.csv")
PROCESSED_DATA_PATH = Path("data/processed/selected_columns.csv")
DEFAULT_METRICS_PATH = Path("outputs/metrics/metrics.json")

SELECTED_COLUMNS: List[str] = [
    "_MICHD",
    "CHOLMED3",
    "GENHLTH",
    "MAXVO21_",
    "EMPLOY1",
    "BPHIGH6",
    "DIABETE4",
    "CVDSTRK3",
    "_AGE80",
    "VETERAN3",
    "CHCCOPD3",
    "CHILDREN",
    "_DRDXAR2",
    "DEAF",
    "DIFFWALK",
    "TOLDHI3",
    "PHYSHLTH",
    "CHCKDNY2",
    "FALL12MN",
    "_RFCHOL3",
    "TRNSGNDR",
    "PVTRESD3",
    "PRIMINS1",
    "HAVARTH4",
    "DIFFALON",
    "SMOKE100",
    "ASTHMA3",
    "LANDLINE",
    "CHCOCNC1",
    TARGET_COLUMN,
]
