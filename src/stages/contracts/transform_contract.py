from dataclasses import dataclass, field
from typing import Dict, List, Tuple

import pandas as pd


@dataclass(frozen=True)
class TransformContractGCS:
    GCS_RAW_LOAD_CONTENT: List[Dict[str, any]] = field(default_factory=list)

@dataclass
class TransformContractBigQuery:
    BQ_LOAD_CONTENT: pd.DataFrame = field(default_factory=pd.DataFrame)
    