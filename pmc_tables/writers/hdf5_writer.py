import logging
import re
from collections import Counter
from typing import List, Optional, Tuple

import pandas as pd

import pmc_tables

logger = logging.getLogger(__name__)


def write_hdf5_table_forcefully(key, df, store, max_tries=None, tried_fns=()):
    if max_tries is None:
        max_tries = len(df.columns) + 1
    try:
        return pmc_tables.write_hdf5_table(key, df, store)
    except Exception as e:
        logger.warning("Encountered error `%s`", e)
        if max_tries == 0:
            raise e
        # Try some fixes
        for fixer_fn in fixer_functions():
            try:
                fixed_df = fixer_fn(df, str(e))
                pmc_tables.write_hdf5_table(key, fixed_df, store)
            except Exception:
                continue
        raise e


def fixer_functions():
    # yield _fix_extra_headers_and_footers
    pass


class _UnhandledError(Exception):
    pass


def _fix_extra_headers_and_footers(df: pd.DataFrame, error: Optional[str]=None):
    if error and not error.startswith("Cannot serializez the column"):
        raise _UnhandledError()
    # Get a mask for mied columns
    mixed_columns = _find_mixed_columns(df)
    is_number_mask = _get_is_number_mask(df, mixed_columns)
    # Make sure at least 90% of mixed columns are numbers
    _check_mostly_number(is_number_mask)
    # Extract the header and / or the footer
    header_slice, footer_slice = _find_header_and_footer(is_number_mask)
    df = _add_rows_to_header(df, row_idxs=header_slice)
    df = df.drop(pd.Index(range(*footer_slice)), axis=0)
    # Make sure columns can be floats now...
    df = _format_mixed_columns(df, mixed_columns)
    return df


def _find_mixed_columns(df: pd.DataFrame) -> List[str]:
    object_columns = df.dtypes[df.dtypes == object].index.tolist()
    mixed_columns = [
        c for c in object_columns if any(isinstance(v, (int, float)) for v in df[c].values)
    ]
    return mixed_columns


def _get_is_number_mask(df: pd.DataFrame, mixed_columns: List[str]) -> pd.Series:
    is_number_df = pd.DataFrame(
        {c: [isinstance(v, (int, float)) for v in df[c].values]
         for c in mixed_columns},
        columns=mixed_columns)
    is_number_s = is_number_df.all(axis=1)
    return is_number_s


def _check_mostly_number(is_number_mask, cutoff=0.9):
    is_number_count = Counter(is_number_mask)
    frac_number = is_number_count[True] / (is_number_count[True] + is_number_count[False])
    if frac_number < 0.9:
        raise _UnhandledError(
            f"The fraction of numbers in mixed columns is too low ({frac_number}).")


def _find_header_and_footer(is_number_s: pd.Series) -> Tuple[Tuple[int, int], Tuple[int, int]]:
    header_start = 0
    header_stop = None
    footer_start = None
    footer_stop = len(is_number_s)
    for i in range(len(is_number_s)):
        if header_stop is None and is_number_s[i]:
            header_stop = i
        elif footer_start is None and not is_number_s[i]:
            footer_start = i
    max_permissible_offset = min(5, len(is_number_s) // 20)
    if ((header_stop - header_start) > max_permissible_offset or
        (footer_stop - footer_start) > max_permissible_offset):
        raise _UnhandledError("Either the header or the footer is larger than allowed.")
    return (header_start, header_stop), (footer_start, footer_stop)


def _add_rows_to_header(df, row_idxs=(0,)):
    columns = list(df.columns)
    assert all(isinstance(c, str) for c in columns)
    for i in row_idxs:
        for j in range(len(columns)):
            columns[i] = (columns[i], df.iloc[i, j])
    df = df.drop(pd.Index(df.index[row_idxs]), axis=0)
    df.columns = pmc_tables.format_columns(columns)
    return df


def _format_mixed_columns(df, columns=None):
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


def _fix_serialize_column_error_bak(error, key, df, store, max_tries, tried_fns):
    match = re.findall(
        "Cannot serialize the column \[(.*)\] because\nits data contents are \[(.*)\] object dtype",
        error)
    if not match:
        return None
    column_name, column_dtype = match[0]
    if 'mixed' in column_dtype and tried_fns.count(pmc_tables.add_first_row_to_header) < 1:
        try:
            _df = pmc_tables.add_first_row_to_header(df)
            return write_hdf5_table_forcefully(key, _df, store, max_tries + 1,
                                               tried_fns + (pmc_tables.add_first_row_to_header,))
        except Exception:
            pass
    if column_dtype in ['integer', 'floating']:
        _df = df.copy()
        _df[column_name] = _df[column_name].astype(float)
        return write_hdf5_table_forcefully(key, _df, store, max_tries, tried_fns)
    elif column_dtype in ['mixed-integer']:
        try:
            _df = df.copy()
            _df[column_name] = _df[column_name].astype(float)
        except Exception:
            pass
        else:
            return write_hdf5_table_forcefully(key, _df, store, max_tries, tried_fns)
    logger.warning("Converting column `%s` with values `%s` to str", column_name,
                   df[column_name].values[:10])
    _df = df.copy()
    _df[column_name] = _df[column_name].astype(str)
    return write_hdf5_table_forcefully(key, _df, store, max_tries, tried_fns)
