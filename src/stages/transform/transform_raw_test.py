import pandas as pd

from src.config import SwAPIConfig as Config
from src.drivers.mocks import mock_extractor_list
from src.errors import TransformError
from src.stages.contracts import TransformContractBigQuery

from .transform_raw import TransformRaw


def test_transform():
    transform_raw = TransformRaw()

    df1, df2 = transform_raw.transform(list(mock_extractor_list()))

    assert isinstance(df1, TransformContractBigQuery)
    assert isinstance(df2, TransformContractBigQuery)
    assert isinstance(df1.BQ_LOAD_CONTENT, pd.DataFrame)
    assert isinstance(df2.BQ_LOAD_CONTENT, pd.DataFrame)
    assert list(df2.BQ_LOAD_CONTENT.columns) == Config.FILTER_COLS

def test_transform_error():
    transform_raw = TransformRaw()
    try:
        transform_raw.transform("error input")
    except Exception as e:
        assert isinstance(e, TransformError)
