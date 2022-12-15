from datetime import datetime

from src.stages.contracts import ExtractContract

from .http_requester_spy import HTTPRequesterSpy


class SwAPIExtractorSpy:

    def __init__(self, http_requester: HTTPRequesterSpy) -> None:
        self.requester = http_requester

    def request_starships_by_page(self) -> ExtractContract:

        return self.requester.request_data()
