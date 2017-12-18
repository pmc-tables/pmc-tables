import pandas as pd
import pytest

from pmc_tables.fixers import column_dtypes as fix

TEST_DATA = [{
    'df':
    pd.DataFrame(
        [[1, 1.1, 'a'], [2, 2.2, 'b']],
        columns=[('c1', 'h1'), ('c2', 'h2'), ('c3', 'h3')],
        index=[1, 2],
        dtype=object),
    'df_fixed':
    pd.DataFrame(
        [[1, 1.1, 'a'], [2, 2.2, 'b']],
        columns=[('c1', 'h1'), ('c2', 'h2'), ('c3', 'h3')],
        index=[1, 2]),
}]


@pytest.parametrize("df, df_fixed", TEST_DATA)
def test_fix_column_dtypes(df, df_fixed):
    df_fixed_ = fix.fix_column_dtypes(df, {})
    assert df_fixed_.equals(df_fixed)
