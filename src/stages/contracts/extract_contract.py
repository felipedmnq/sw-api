from dataclasses import dataclass, field
from typing import Dict


@dataclass(frozen=True)
class ExtractContract:
    EXTRACT_DATETIME: str
    RAW_DATA: Dict[str, any] = field(default_factory=dict)
    