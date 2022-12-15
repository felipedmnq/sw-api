from typing import Dict

from src.drivers.mocks.sw_api_extractor_mock import mock_extractor


class HTTPRequesterSpy:

    def __init__(self) -> None:
        self.request_data_count = 0

    def request_data(self) -> Dict[str, any]:
        self.request_data_count += 1
        return mock_extractor()