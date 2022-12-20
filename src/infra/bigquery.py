from typing import List

from google.cloud import bigquery
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