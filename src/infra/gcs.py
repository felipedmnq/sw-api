import json
from datetime import datetime, timezone
from io import BytesIO
from typing import Dict

from google.cloud import storage
from google.cloud.storage.bucket import Bucket

from src.config import SwAPIConfig as Config
from src.stages.contracts import TransformContractGCS


class CloudStorage:

    def __init__(self, bucket: str, service_account_path: str = None) -> None:
        self.bucket = bucket
        self.service_account_path = service_account_path

    def __get_bucket(self) -> Bucket:
        if self.service_account_path:
            client = storage.Client.from_service_account_json(
                self.service_account_path
            )
        else:
            client  = storage.Client()

        return client.get_bucket(self.bucket)

    def __to_json(self, obj) -> Dict[str, any]:

        return json.dumps(obj, indent=4, allow_nan=True)

    def __create_file_path(self, metadata: bool = False) -> str:
        path = Config.GCS_DUMP_DIR
        base_name = Config.RAW_DATA_FILE_NAME
        file_ext = ".json"

        current_datetime = datetime.now(timezone.utc)
        current_time = current_datetime.strftime(format=Config.TIME_FORMAT)[:-3]
        current_date = current_datetime.strftime(format=Config.DATE_FORMAT)

        filename = "_".join([base_name, current_date, current_time]) + file_ext
        if metadata:
            return f"{path}/{current_date}/metadata/{filename}"

        return f"{path}/{current_date}/raw_data/{filename}"

    def __dump_data(
        self,
        gcs_path: str,
        file_object: TransformContractGCS,
        timeout: int
    ) -> None:

        bucket = self.__get_bucket()
        blob = bucket.blob(gcs_path)

        blob.upload_from_string(file_object, timeout=timeout)

    def __dump_to_gcs(
        self,
        file_object: TransformContractGCS,
        gcs_path: str,
        timeout: int = 60,
        max_iterations: int = Config.GCS_MAX_ITERATIONS
    ) -> None:
        iterations = 0

        while iterations < max_iterations:
            try:
                self.__dump_data(gcs_path, file_object, timeout)
                break

            except Exception as e:
                print(f"\033[91m{e}\033[0m")
                pass ## CREATE AN CUSTOM ERROR

    def dump_to_gcs_bucket(
        self, file_object, timeout: int = 60, metadata=False
    ) -> None:

        file_object = self.__to_json(file_object)
        gcs_dump_path = self.__create_file_path(metadata)

        self.__dump_to_gcs(file_object, gcs_dump_path, timeout)
