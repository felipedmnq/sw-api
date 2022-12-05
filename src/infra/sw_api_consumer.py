from typing import Dict

from requests import Session

from src.config import SwAPIConfig as Config
from src.extrac import Extractor


class SwAPIConsumer:

    def __init__(self) -> None:
        self.extractor = Extractor()
        self.session = self.extractor.create_session()

    def request_starships(self, page: int, session: Session) -> Dict:

        uri = f"{Config.BASE_URI}/starships/{page}"

        return session.get(uri)