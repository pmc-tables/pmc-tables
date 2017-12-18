import pandas as pd
import pytest

from pmc_tables.fixers import extra_headers_and_footers as fix
from pmc_tables.fixers import _FixDoesNotApplyError

TEST_DATA = [{
    'df':
    pd.DataFrame(
        [['h1', 'h2', 'h3'], [1, 1.1, 'a'], [2, 2.2, 'b']],
        columns=['c1', 'c2', 'c3'],
        index=[0, 1, 2]),
    'df_fixed':
    pd.DataFrame(
        [[1, 1.1, 'a'], [2, 2.2, 'b']],
        columns=[('c1', 'h1'), ('c2', 'h2'), ('c3', 'h3')],
        index=[1, 2],
        dtype=object),
    'is_number_mask':
    pd.Series([False, True, True], index=[0, 1, 2]),
    'header_range':
    range(0, 1),
    'footer_range':
    range(3, 3),
}]


@pytest.parametrize("df, is_number_mask", TEST_DATA)
def test__get_is_number_mask(df, is_number_mask):
    is_number_mask_ = fix._get_is_number_mask(df)
    assert is_number_mask_.equals(is_number_mask)


@pytest.parametrize("is_number_mask, header_range, footer_range", TEST_DATA)
def test__find_header_and_footer(is_number_mask, header_range, footer_range):
    header_range_, footer_range_ = fix._find_header_and_footer(is_number_mask)
    assert header_range_ == header_range
    assert footer_range_ == footer_range


@pytest.parametrize("is_number_mask", TEST_DATA)
def test__check_mostly_numbers(is_number_mask):
    fix._check_mostly_numbers(is_number_mask)
    fix._check_mostly_numbers(is_number_mask, cutoff=0)
    fix._check_mostly_numbers(is_number_mask, cutoff=1)


@pytest.parametrize("df, df_fixed", TEST_DATA)
def test_fix_extra_headers_and_footers(df, df_fixed):
    with pytest.raises(_FixDoesNotApplyError):
        fix.fix_extra_headers_and_footers(df.copy(), {})
    with pytest.raises(_FixDoesNotApplyError):
        fix.fix_extra_headers_and_footers(df.copy(), {'error_message': "Something bad happened..."})
    df_fixed_ = fix.fix_extra_headers_and_footers(df.copy(), {
        'error_message': "Cannot serialize the column ..."
    })
    assert df_fixed_.equals(df_fixed)
