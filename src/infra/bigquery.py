from typing import Dict, List

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
from google.oauth2 import service_account

from src.config import SwAPIConfig as Config


class BigQuery:
    date_format = '%Y-%m-%d'

    def __init__(self, service_account_path: str = None) -> None:
        self.service_account_path = service_account_path
        self.project_id = Config.GCP_PROJECT_ID

        if service_account_path:
            self.credentials = self.get_credentials()

        self.bq_client = self.get_client()


    def get_credentials(self):
        return service_account.Credentials.from_service_account_file(self.service_account_path)

    def get_client(self):
        return bigquery.Client(credentials=self.credentials, project=self.project_id)

    def list_dataset_tables(self, dataset_name: str) -> List[str]:
        """List all table names inside a BQ dataset"""
        BQtables = self.bq_client.list_tables(dataset_name)
        tables_list = [table.table_id for table in BQtables]

        if isinstance(tables_list, list) and len(tables_list) > 0:
            return tables_list
        else:
            print(f"\033[91mNo tables to list in {dataset_name} dataset.\033[0m")
        # else:
        #     raise EmptyDatasetError(self, dataset_name=dataset_name)

    def __dataset_exists(self, dataset: str) -> bool:
        bq_dataset = self.bq_client.dataset(dataset)
        return self.__exist(self.bq_client.get_dataset, bq_dataset)

    def __table_exists(self, dataset: str, table_name: str) -> bool:
        if not self.__dataset_exists(dataset):
            return False

        dataset_table = self.bq_client.dataset(dataset).table(table_name)
        return self.__exist(self.bq_client.get_table, dataset_table)

    def __map_fields_type(self, table_schema: Dict[str, any]) -> Dict[str, any]:
        bq_field_type = Config.BQ_FIELD_TYPES_MAP
        tb_schema = []
        for col, dtype in table_schema.items():
            tb_schema.append(SchemaField(col, bq_field_type[dtype]))

        return tb_schema

    def __create_table(
        self,
        dataset: str,
        table_name: str,
        schema: Dict[str, any]
    ) -> None:
        tb_schema = self.__map_fields_type(schema)
        full_table_path = self.__bq_full_path(dataset, table_name)

        bq_table = bigquery.Table(full_table_path, tb_schema)
        self.bq_client.create_table(bq_table)

    def __insert_df_to_bq_table(
        self,
        df: pd.DataFrame,
        dataset_name: str,
        table_name: str
    ) -> None:
        bq_table_path = self.__bq_full_path(dataset_name, table_name)
        self.bq_client.load_table_from_dataframe(df, bq_table_path).result()

    def __exist(self, fun, arg):
        try:
            fun(arg)
            return True
        except:
            return False

    def __bq_full_path(self, dataset: str, table_name: str) -> str:
        return ".".join([self.project_id, dataset, table_name])

    def insert_df(
        self,
        df: pd.DataFrame,
        dataset_name: str,
        table_name: str,
        schema: Dict[str, any]
    ) -> None:
        if not self.__table_exists(dataset_name, table_name):
            self.__create_table(dataset_name, table_name, schema)

        self.__insert_df_to_bq_table(df, dataset_name, table_name)