from typing import Dict, List

import numpy as np
import pandas as pd

from src.config import SwAPIConfig as Config
from src.errors import TransformError
from src.infra import get_env_variable
from src.stages import BigQueryLoader, GCSLoader
from src.stages.contracts import (ExtractContract, TransformContractBigQuery,
                                  TransformContractGCS)


class TransformRaw:

    def transform(self, starships_list: List[ExtractContract]) -> None:
        try:
            return self.__filter_and_transform(starships_list)
        except:
            raise TransformError(self)

    def __extract_metadata(self, starship_data: ExtractContract) -> Dict[str, any]:
        return {
                "query_id": starship_data.QUERY_ID,
                "extract_date": starship_data.EXTRACT_DATETIME,
                "page": int(starship_data.PAGE)
            }

    def __extract_starship_data(self, starship_data: ExtractContract) -> Dict[str, any]:
        starship_properties = starship_data.RAW_DATA["properties"]

        data_dict = {
            key: value for key, value in starship_properties.items()
            if key in Config.FILTER_COLS
        }
        
        data_dict["query_id"] = starship_data.QUERY_ID
        data_dict["processing_timestamp"] = starship_data.EXTRACT_DATETIME

        return data_dict

    def __create_dataframe(self, data_list: List[Dict[str, any]]) -> pd.DataFrame:
        return pd.DataFrame(data_list)

    def __cast_dtypes(self, df: pd.DataFrame, schema_config: Dict[str, any]) -> pd.DataFrame:
        df = df.astype({
            key: value for key, value in schema_config.items()
            if key in df.columns
        })

        return df

    def __replace_nan(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.replace(
            ["unknown", "None", "nan", " ", "", "n/a"], np.nan
        )

    def __split_crew(self, df: pd.DataFrame) -> pd.DataFrame:
        if "crew" in df.columns:
            df["crew"] = df["crew"].apply(lambda cell: cell.split("-")[-1] if "-" in cell else cell)

        return df

    def __replace_coma(
        self,
        df: pd.DataFrame,
        schema: Dict[str, any]
    ) -> pd.DataFrame:
        for col, dtype in schema.items():
            if dtype == float:
                df[col] = df[col].apply(lambda cell: cell.replace(",", "."))
            elif dtype == pd.Int64Dtype():
                df[col] = df[col].apply(lambda cell: cell.replace(",", ""))
                df[col] = df[col].apply(lambda x: ''.join([n for n in x if n.isdigit()]))
                # df[col] = df[col].apply(lambda x: ''.join([n for n in x if n.isdigit()]) if not x.isdigit() else x0)

        return df

    def __filter_and_transform(
        self,
        starships_list: List[ExtractContract]
    ) -> List[Dict[str, any]]:

        gcs_loader = GCSLoader()

        bq_loader = BigQueryLoader()

        bq_metadata_contract = TransformContractBigQuery()
        bq_data_contract = TransformContractBigQuery()

        metadata_contract = TransformContractGCS()
        results_contract = TransformContractGCS()

        schema_config = Config.TABLE_SCHEMA

        for starship in starships_list:
            metadata_contract.GCS_RAW_LOAD_CONTENT.append(self.__extract_metadata(starship))
            results_contract.GCS_RAW_LOAD_CONTENT.append(self.__extract_starship_data(starship))


        gcs_loader.load_data(results_contract)
        gcs_loader.load_data(metadata_contract, metadata=True)

        metadata_df = self.__create_dataframe(metadata_contract.GCS_RAW_LOAD_CONTENT)
        starships_df = self.__create_dataframe(results_contract.GCS_RAW_LOAD_CONTENT)
        starships_df = self.__split_crew(starships_df)
        starships_df = self.__replace_coma(starships_df, schema_config)
        starships_df = starships_df[list(schema_config.keys())]
        starships_df = self.__replace_nan(starships_df)
        metadata_df = self.__cast_dtypes(metadata_df, Config.METADATA_SCHEMA)
        starships_df = self.__cast_dtypes(starships_df, schema_config)
        
        bq_metadata_contract.BQ_LOAD_CONTENT = metadata_df
        bq_data_contract.BQ_LOAD_CONTENT = starships_df
        

        bq_loader.load_data(
            bq_data_contract,
            Config.BIGQUERY_DATASET,
            Config.TABLE_NAME,
            Config.TABLE_SCHEMA
        )
        bq_loader.load_data(
            bq_metadata_contract,
            Config.BIGQUERY_DATASET,
            Config.METADATA_TABLE,
            Config.METADATA_SCHEMA
        )

        return bq_metadata_contract, bq_data_contract

