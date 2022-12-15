from typing import Dict

from src.interfaces.extractor_interface import Extractor


class HTTPRequester(Extractor):

    def __init__(self) -> None:
        self.session = self.create_session()

   
   