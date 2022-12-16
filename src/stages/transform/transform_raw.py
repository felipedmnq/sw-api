from typing import Dict, List

import pandas as pd

from src.config import SwAPIConfig as Config
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
            if key in Config.TABLE_SCHEMA.keys()
        }

    def __create_dataframe(self, data_list: List[Dict[str, any]]) -> pd.DataFrame:
        return pd.DataFrame(data_list)

    def __filter_and_transform(
        self,
        starships_list: List[ExtractContract]
    ) -> List[Dict[str, any]]:

        metadata = []
        results = []

        for starship in starships_list:
            metadata.append(self.__extract_metadata(starship))
            results.append(self.__extract_starship_data(starship))

        metadata_df = self.__create_dataframe(metadata)
        starships_df = self.__create_dataframe(results)
        starships_df = starships_df[list(Config.TABLE_SCHEMA.keys())[2:]]
            
        print(f"\033[91m{metadata_df}\033[0m")
        print(f"\033[96m{starships_df}\033[0m")

        return metadata_df, starships_df

