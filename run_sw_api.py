import pandas as pd

from src.drivers import HTTPRequester
from src.stages.extract.sw_api_extractor import SwAPIExtractor
from src.stages.transform import TransformRaw

if __name__=="__main__":
    http_requester = HTTPRequester()
    api_consumer = SwAPIExtractor(http_requester)
    tranformer = TransformRaw()
    
    pages = [page for page in range(1, 40)]
    # print(f"\033[91m{pages}\033[0m")

    starships_list = api_consumer.extract_starships(pages)
    transformed_data = tranformer.transform(starships_list)
    # response = api_consumer.request_starships_by_page(2)
    # print(f"\033[92m{dir(response)}\033[0m")