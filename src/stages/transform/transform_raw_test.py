import pandas as pd

from src.drivers.mocks import mock_extractor_list

from .transform_raw import TransformRaw


def test_transform():
    transform_raw = TransformRaw()

    df1, df2 = transform_raw.transform(list(mock_extractor_list()))

    assert isinstance(df1, pd.DataFrame)
    assert isinstance(df2, pd.DataFrame)


