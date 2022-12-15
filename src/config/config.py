from dataclasses import dataclass


@dataclass(frozen=True)
class SwAPIConfig:

    BASE_URI: str = "https://www.swapi.tech/api"
    DATE_FORMAT: str = '%Y-%m-%d'
    BIGQUERY_PROJECT: str = None