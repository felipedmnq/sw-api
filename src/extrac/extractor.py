from abc import ABC
from typing import Tuple

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class Extractor(ABC):

    def __init__(self) -> None:
        pass
        
    @classmethod
    def create_session(
        cls,
        retries: int =3,
        backoff_factor: float = 0.3,
        status_forcelist: Tuple[int] = tuple(range(400, 430)) + (500, 502, 503, 504),
        session: Session = None
    ) -> Session:

        session = session or Session()

        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=frozenset(['GET', 'POST'])
        )

        session_adapter = HTTPAdapter(max_retries=retry)

        session.mount('http://', session_adapter)
        session.mount('https://', session_adapter)

        return session