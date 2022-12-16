import uuid
from datetime import datetime
from typing import List

from src.stages.contracts import ExtractContract


def mock_extractor() -> ExtractContract:
    return ExtractContract(
        QUERY_ID=str(uuid.uuid4()),
        EXTRACT_DATETIME=str(datetime.today()),
        PAGE=1,
        RAW_DATA={
            'properties': {
                'model': 'CR90 corvette',
                'starship_class': 'corvette',
                'manufacturer': 'Corellian Engineering Corporation',
                'cost_in_credits': '3500000',
                'length': '150',
                'crew': '30-165',
                'passengers': '600', 
                'max_atmosphering_speed': '950',
                'hyperdrive_rating': '2.0',
                'MGLT': '60',
                'cargo_capacity': '3000000',
                'consumables': '1 year', 
                'pilots': [],
                'created': '2020-09-17T17:55:06.604Z',
                'edited': '2020-09-17T17:55:06.604Z',
                'name': 'CR90 corvette', 
                'url': 'https://www.swapi.tech/api/starships/2',
                'description': 'A Starship',
                '_id': '5f63a34fee9fd7000499be1e',
                'uid': '2', '__v': 0
            }
        }
    )

def mock_extractor_list() -> List[ExtractContract]:
    return [mock_extractor()]