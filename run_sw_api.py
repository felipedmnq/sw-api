import pandas as pd

from src.extrac import Extractor
from src.infra.sw_api_consumer import SwAPIConsumer

if __name__=="__main__":
    api_consumer = SwAPIConsumer()
    extractor = Extractor()
    session = extractor.create_session()

    page = 2

    response = api_consumer.request_starships(page, session)

    print(f"\033[91m{response.json()}\033[0m")