from src.config import SwAPIConfig as Config
from src.drivers import HTTPRequester

from .sw_api_consumer import SwAPIConsumer


def test_request_starships(requests_mock):

    page = 2

    url = f"{Config.BASE_URI}/starships/{page}"
    response_json = {"status_code": 200, "json_response": "check test"}

    requests_mock.get(url, status_code=200, json=response_json)
    http_requester = HTTPRequester()
    request_response = http_requester.request_data(url)

    assert "status_code" in request_response
    assert request_response["status_code"] == 200
    assert request_response["json_response"] == response_json["json_response"]