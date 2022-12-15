from typing import Dict

from requests import Session

from src.config import SwAPIConfig as Config
from src.drivers import HTTPRequester


class SwAPIConsumer:

    def __init__(self) -> None:
        self.requester = HTTPRequester()

    def request_starships(self, page: int) -> Dict:

        url = f"{Config.BASE_URI}/starships/{page}"

        return self.requester.request_data(url)