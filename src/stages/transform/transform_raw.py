from typing import Dict, List

import numpy as np
import pandas as pd

from src.config import SwAPIConfig as Config
from src.infra import CloudStorage, get_env_variable
from src.stages.contracts import ExtractContract


class TransformRaw:

    def transform(self, starships_list: List[ExtractContract]) -> None:
        return self.__filter_and_transform(starships_list)

    def __extract_metadata(self, starship_data: ExtractContract) -> Dict[str, any]:
        return {
                "query_id": starship_data.QUERY_ID,
                "extract_date": starship_data.EXTRACT_DATETIME,
                "page": starship_data.PAGE
            }

    def __extract_starship_data(self, starship_data: ExtractContract) -> Dict[str, any]:
        starship_properties = starship_data.RAW_DATA["properties"]

        return {
            key: value for key, value in starship_properties.items()
            if key in Config.FILTER_COLS
        }

    def __create_dataframe(self, data_list: List[Dict[str, any]]) -> pd.DataFrame:
        return pd.DataFrame(data_list)

    def __cast_dtypes(self, df: pd.DataFrame, schema_config: Dict[str, any]) -> pd.DataFrame:
        df = df.astype({
            key: value for key, value in schema_config.items()
            if key in df.columns
        })

        return df

    def __replace_nan(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.replace(['None', 'nan', ' ', 'n/a'], np.nan)

    def __split_crew(self, df: pd.DataFrame) -> pd.DataFrame:
        if "crew" in df.columns:
            df["crew"] = df["crew"].apply(lambda cell: cell.split("-")[-1] if "-" in cell else cell)

        return df

    def __replace_coma(self, df: pd.DataFrame) -> pd.DataFrame:
        if "crew" in df.columns:
            df["crew"] = df["crew"].apply(lambda cell: cell.replace(",", "") if "," in cell else cell)
        
        if "length" in df.columns:
            df["length"] = df["length"].apply(lambda cell: cell.replace(",", ".") if "," in cell else cell)

        return df


    def __filter_and_transform(
        self,
        starships_list: List[ExtractContract]
    ) -> List[Dict[str, any]]:

        gcs = CloudStorage(
            Config.GCS_BUCKET, get_env_variable(Config.SW_GCP_SERVICE_ACCOUNT)
        )

        metadata = []
        results = []
        results_dict = {"results": results}

        for starship in starships_list:
            metadata.append(self.__extract_metadata(starship))
            results.append(self.__extract_starship_data(starship))

        raw_data = {"metadata": metadata, "sw_data": results}
        gcs.dump_to_gcs_bucket(results)

        metadata_df = self.__create_dataframe(metadata)

        starships_df = self.__create_dataframe(results)
        starships_df = self.__replace_coma(starships_df)
        starships_df = self.__split_crew(starships_df)

        schema_config = Config.TABLE_SCHEMA

        starships_df = starships_df[list(schema_config.keys())[2:]]
        starships_df = self.__replace_nan(starships_df)
        starships_df = self.__cast_dtypes(starships_df, schema_config)

        return metadata_df, starships_df

