from src.config import SwAPIConfig as Config
from src.infra import CloudStorage, get_env_variable
from src.interfaces import Loader
from src.stages.contracts import TransformContractGCS


class GCSLoader(Loader):
    def load_data(self, gcs_contract: TransformContractGCS, metadata: bool = False) -> None:
        gcs = CloudStorage(
            Config.GCS_BUCKET, get_env_variable(Config.SW_GCP_SERVICE_ACCOUNT)
        )
        data = gcs_contract.GCS_RAW_LOAD_CONTENT
        gcs.dump_to_gcs_bucket(data, metadata)