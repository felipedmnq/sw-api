import uuid
from datetime import datetime
from typing import Dict, List

from src.config import SwAPIConfig as Config
from src.drivers import HTTPRequester
from src.errors import ExtractError
from src.stages.contracts import ExtractContract


class SwAPIExtractor:

    def __init__(self, http_requester: HTTPRequester) -> None:
        self.requester = http_requester

    def request_starships_by_page(self, page: int) -> ExtractContract:

        url = f"{Config.BASE_URI}/starships/{page}/"
        try:
            raw_data = ExtractContract(
                QUERY_ID=str(uuid.uuid4()),
                EXTRACT_DATETIME=str(datetime.today()),
                PAGE=page,
                RAW_DATA=self.requester.request_data(url)["result"],
            )
        except Exception as e:
            raise ExtractError(str(e))

        return raw_data

    def extract_starships(self, pages: List[ExtractContract]):

        results_list = []
        for page in pages:
            try:
                results_list.append(self.request_starships_by_page(page))
            except:
                ## INSERT A LOG INFORMATION HERE
                print(f"\033[91mNot possible to retrieve data from page {page}\033[0m")
                continue

        return results_list