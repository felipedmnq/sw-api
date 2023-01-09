from typing import Dict

import pandas as pd

from src.config import SwAPIConfig as Config
from src.infra import BigQuery, get_env_variable
from src.interfaces import Loader
from src.stages.contracts import TransformContractBigQuery


class BigQueryLoader(Loader):

    def load_data(
        self,
        bigquery_contract: TransformContractBigQuery,
        dataset_name: str,
        table_name: str,
        schema: Dict[str, any]
    ) -> None:
        data = bigquery_contract.BQ_LOAD_CONTENT
        bq = BigQuery(get_env_variable(Config.SW_GCP_SERVICE_ACCOUNT))
        bq.insert_df(data, dataset_name, table_name, schema)