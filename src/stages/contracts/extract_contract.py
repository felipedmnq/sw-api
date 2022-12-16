from dataclasses import dataclass, field
from typing import Dict


@dataclass(frozen=True)
class ExtractContract:
    QUERY_ID: str
    EXTRACT_DATETIME: str
    PAGE: int
    RAW_DATA: Dict[str, any] = field(default_factory=dict)
    