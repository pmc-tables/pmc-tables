import pandas as pd
import pytest

import pmc_tables
import pmc_tables.writers.hdf5_writer

from typing import List

TEST_DATA = [{
    'df': pd.DataFrame(
        [['h1', 'h2', 'h3'], [1, 1.1, 'a'], [2, 2.2, 'b']],
        columns=['c1', 'c2', 'c3'],
        index=[0, 1, 2]),
    'df_fixed': pd.DataFrame(
        [[1, 1.1, 'a'], [2, 2.2, 'b']], columns=['c1 | h1', 'c2 | h2', 'c3 | h3'], index=[1, 2]),
    'is_number_mask': pd.Series(
        [False, True, True], index=[0, 1, 2]),
    'header_range': range(0, 1),
    'footer_range': range(3, 3),
}]


def parametrize(arg_string: str, data: List[dict]):
    args = [a.strip() for a in arg_string.split(',')]
    inputs = [tuple(d[arg] for arg in args) if len(args) > 1 else d[args[0]] for d in data]
    return pytest.mark.parametrize(arg_string, inputs)


@parametrize("df, is_number_mask", TEST_DATA)
def test__get_is_number_mask(df, is_number_mask):
    is_number_mask_ = pmc_tables.writers.hdf5_writer._get_is_number_mask(df)
    assert is_number_mask_.equals(is_number_mask)


@parametrize("is_number_mask, header_range, footer_range", TEST_DATA)
def test__find_header_and_footer(is_number_mask, header_range, footer_range):
    header_range_, footer_range_ = pmc_tables.writers.hdf5_writer._find_header_and_footer(
        is_number_mask)
    assert header_range_ == header_range
    assert footer_range_ == footer_range


@parametrize("is_number_mask", TEST_DATA)
def test__check_mostly_numbers(is_number_mask):
    pmc_tables.writers.hdf5_writer._check_mostly_numbers(is_number_mask)
    pmc_tables.writers.hdf5_writer._check_mostly_numbers(is_number_mask, cutoff=0)
    pmc_tables.writers.hdf5_writer._check_mostly_numbers(is_number_mask, cutoff=1)


@parametrize("df, df_fixed", TEST_DATA)
def test_fix_extra_headers_and_footers(df, df_fixed):
    df_fixed_ = pmc_tables.fix_extra_headers_and_footers(df)
    assert df_fixed_.equals(df_fixed)
