import datetime
from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SwAPIConfig:

    BASE_URI: str = "https://www.swapi.tech/api"
    GCP_PROJECT_ID = "light-reality-344611"
    SW_GCP_SERVICE_ACCOUNT = "SW_GCP_SERVICE_ACCOUNT"
    DATE_FORMAT: str = "%Y-%m-%d"
    BIGQUERY_PROJECT: str = None
    BIGQUERY_DATASET = "sw_api"
    TABLE_NAME = "sw_api_table"
    METADATA_TABLE = "sw_api_metadata"
    TIME_FORMAT: str = "%H%M%S%f"
    GCS_BUCKET = "datalake-felipedmnq"
    RAW_DATA_FILE_NAME: str = "raw_sw_api"
    GCS_DUMP_DIR = "sw_api/raw_data"
    GCS_MAX_ITERATIONS = 3

    BQ_FIELD_TYPES_MAP = {
        str: 'STRING',
        bytes: 'BYTES',
        int: 'INTEGER',
        float: 'FLOAT',
        bool: 'BOOLEAN',
        pd.Int64Dtype(): 'INTEGER',
        datetime.datetime: 'DATETIME',
        datetime.date: 'DATE',
        datetime.time: 'TIME',
        np.datetime64: 'DATETIME',
        np.timedelta64: 'TIME'}

    FILTER_COLS = [
        "query_id", "processing_timestamp", "model", "name", "starship_class",
        "manufacturer", "length", "crew", "passengers", "max_atmosphering_speed",
        "hyperdrive_rating", "cargo_capacity"
    ]

    METADATA_SCHEMA = {
        "query_id": str,
        "extract_date": np.datetime64,
        "page": pd.Int64Dtype()
    }

    TABLE_SCHEMA = {
        "query_id": str,
        "processing_timestamp": np.datetime64,
        "model": str,
        "name": str,
        "starship_class": str,
        "manufacturer": str,
        "length": float,
        "crew": pd.Int64Dtype(),
        "passengers": pd.Int64Dtype(),
        "max_atmosphering_speed": pd.Int64Dtype(),
        "hyperdrive_rating": float,
        "cargo_capacity": pd.Int64Dtype()
    }