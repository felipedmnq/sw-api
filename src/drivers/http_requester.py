from typing import Dict

from .extractor import Extractor


class HTTPRequester:

    def __init__(self) -> None:
        self.session = Extractor().create_session()

    def request_data(self, url: str) -> Dict[str, any]:

        response = self.session.get(url)

        return response.json()