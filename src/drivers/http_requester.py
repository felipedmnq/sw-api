from typing import Dict

from src.interfaces.http_request_interface import HTTPRequestInterface


class HTTPRequester(HTTPRequestInterface):

    def __init__(self) -> None:
        self.session = self.create_session()

    def request_data(self, url: str) -> Dict[str, any]:

        response = self.session.get(url)
        response.raise_for_status()

        return response.json()