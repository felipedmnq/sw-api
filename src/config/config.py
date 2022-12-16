from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SwAPIConfig:

    BASE_URI: str = "https://www.swapi.tech/api"
    DATE_FORMAT: str = '%Y-%m-%d'
    BIGQUERY_PROJECT: str = None

    TABLE_SCHEMA = {
        "query_id": str,
        "processing_timestamp": np.datetime64,
        "model": str,
        "name": str,
        "starship_class": str,
        "manufacturer": str,
        "length": float,
        "crew": str,
        "passengers": pd.Int64Dtype(),
        "max_atmosphering_speed": pd.Int64Dtype(),
        "hyperdrive_rating": float,
        "cargo_capacity": pd.Int64Dtype()
    }