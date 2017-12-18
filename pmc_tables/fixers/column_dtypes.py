import logging
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


def fix_column_dtypes(df: pd.DataFrame, info: dict) -> pd.DataFrame:
    df = _format_mixed_columns(df)
    return df


def _format_mixed_columns(df: pd.DataFrame, columns: List[str] = None):
    if columns is None:
        columns = list(df.columns)
    for c in columns:
        if all(isinstance(v, int) for v in df[c].values):
            df[c] = df[c].astype(int)
        elif all(isinstance(v, (int, float, type(None))) for v in df[c].values):
            df[c] = df[c].astype(float)
        else:
            logger.debug("Converting column `%s` with values `%s` to `str`.", c, df[c].values[:10])
            df[c] = df[c].astype(str)
    return df
