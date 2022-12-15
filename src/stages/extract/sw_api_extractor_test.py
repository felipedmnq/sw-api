from src.drivers import mock_extractor
from src.drivers.tests import HTTPRequesterSpy, SwAPIExtractorSpy
from src.errors import ExtractError
from src.stages.contracts import ExtractContract
from src.stages.extract import SwAPIExtractor


def test_request_starships_by_page():

    requester = HTTPRequesterSpy()
    request_response = requester.request_data()

    assert isinstance(request_response, ExtractContract)
    assert isinstance(request_response.RAW_DATA, dict)
    assert isinstance(request_response.EXTRACT_DATETIME, str)
    assert requester.request_data_count == 1

def test_extract_starships():
    pages = [1, 2]

    requester = HTTPRequesterSpy()
    extractor = SwAPIExtractorSpy(requester)

    fake_results = []
    for _ in pages:
        fake_results.append(extractor.request_starships_by_page())
    
    print(type(fake_results[0].RAW_DATA))

    assert isinstance(fake_results[0], ExtractContract)
    assert isinstance(fake_results[0].EXTRACT_DATETIME, str)
    assert isinstance(fake_results[0].RAW_DATA, dict)


def test_swapi_extractor_error():
    page = 2
    http_requester = "will trigger the error"

    try:
        extractor = SwAPIExtractor(http_requester)
        extractor.request_starships_by_page(page)
    except Exception as e:
        assert isinstance(e, ExtractError)
