from src.config import SwAPIConfig as Config

from .http_requester import HTTPRequester


def test_request_data(requests_mock):
    url = Config.BASE_URI
    response_json = {"status_code": 200, "json_response": "check test"}

    requests_mock.get(url, status_code=200, json=response_json)
    http_requester = HTTPRequester()
    request_response = http_requester.request_data(url)

    print(f"\033[91m{request_response}\033[0m")

    assert "status_code" in request_response
    assert request_response["status_code"] == 200
    assert request_response["json_response"] == response_json["json_response"]