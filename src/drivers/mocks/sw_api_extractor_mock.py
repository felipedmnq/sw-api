from datetime import datetime
from typing import Dict

from src.stages.contracts import ExtractContract


def mock_extractor() -> Dict[str, any]:
    return ExtractContract(
        EXTRACT_DATETIME=str(datetime.today()),
        RAW_DATA={'message': 'ok', 'result': {}}
    )