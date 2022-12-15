from src.config import SwAPIConfig as Config

from .http_requester import HTTPRequester
from .mocks.http_requester_mock import mock_request


def test_request_data(requests_mock):
    url = f"{Config.BASE_URI}/starships/2"
    response_dict = mock_request()

    requests_mock.get(url, status_code=200, json=response_dict)
    http_requester = HTTPRequester()
    request_response = http_requester.request_data(url)

    print(f"\033[91m{request_response}\033[0m")

    assert isinstance(request_response, dict)
    assert "message" in request_response
    assert "result" in request_response
    assert "properties" in request_response["result"]
    assert request_response["result"]["properties"]["url"] == url