import logging
import re
from collections import Counter
from typing import List, Optional, Tuple

import pandas as pd

import pmc_tables

logger = logging.getLogger(__name__)


class _UnhandledError(Exception):
    pass


def write_hdf5_table_forcefully(key, df, store, max_tries=5, tried_fns=()):
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
                return pmc_tables.write_hdf5_table(key, fixed_df, store)
            # except _UnhandledError:
            #     continue
            except Exception as e2:
                logger.warning("Another error occured trying to fix the first error. (%s: %s)",
                               type(e2), e2)
                raise e2
        raise e


def fixer_functions():
    yield fix_extra_headers_and_footers


def fix_extra_headers_and_footers(df: pd.DataFrame, error: Optional[str]=None):
    if error and not error.startswith("Cannot serialize the column"):
        raise _UnhandledError(f"Unsupported error message: {error}.")
    # Get a mask for mixed columns
    is_number_mask = _get_is_number_mask(df)
    # Make sure at least 90% of mixed columns are numbers
    _check_mostly_numbers(is_number_mask)
    # Extract the header and / or the footer
    header_range, footer_range = _find_header_and_footer(is_number_mask)
    if header_range:
        df = _add_rows_to_header(df, header_range)
    if footer_range:
        df = df.drop(df.index[footer_range], axis=0)
    # Make sure columns can be floats now...
    df = _format_mixed_columns(df)
    return df


def _get_is_number_mask(df: pd.DataFrame) -> pd.Series:
    object_columns = df.dtypes[df.dtypes == object].index.tolist()
    # Mixed columns are all columns taht are not entirely string or null
    is_string_df = pd.DataFrame(
        {c: [(isinstance(v, str) or pd.isnull(v)) for v in df[c].values]
         for c in object_columns},
        columns=object_columns)
    is_string_s = is_string_df.all(axis=0)
    # Number mask indicates whether all mixed columns in a given row are numbers
    mixed_columns = is_string_s[~is_string_s].index.tolist()
    is_number_df = pd.DataFrame(
        {c: [isinstance(v, (int, float)) for v in df[c].values]
         for c in object_columns},
        columns=object_columns)
    is_number_mask = is_number_df[mixed_columns].all(axis=1)
    return is_number_mask


def _check_mostly_numbers(is_number_mask: pd.Series, cutoff: float=0.9) -> None:
    is_number_count = Counter(is_number_mask.values)
    frac_number = is_number_count[True] / (is_number_count[True] + is_number_count[False])
    cutoff = min(cutoff, 1 - 1 / len(is_number_count))
    if frac_number < cutoff:
        raise _UnhandledError(
            f"The fraction of numbers in mixed columns is too low ({frac_number}).")


def _find_header_and_footer(is_number_s: pd.Series) -> Tuple[range, range]:
    header_start = 0
    header_stop = None
    footer_start = None
    footer_stop = len(is_number_s)
    for i in range(len(is_number_s)):
        if header_stop is None and is_number_s[i]:
            header_stop = i
        elif header_stop is not None and footer_start is None and not is_number_s[i]:
            footer_start = i
    max_permissible_offset = min(5, len(is_number_s) // 20 + 1)
    if header_stop is None or (header_stop - header_start) > max_permissible_offset:
        header_stop = 0
    if footer_start is None or (footer_stop - footer_start) > max_permissible_offset:
        footer_start = len(is_number_s)
    return range(header_start, header_stop), range(footer_start, footer_stop)


def _add_rows_to_header(df, header_range=range(0, 1)):
    columns = list(df.columns)
    assert all(isinstance(c, str) for c in columns)
    columns = [(c,) for c in columns]
    for r_idx in header_range:
        for c_idx in range(len(columns)):
            columns[c_idx] = columns[c_idx] + (df.iloc[r_idx, c_idx],)
    df = df.drop(df.index[header_range], axis=0)
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
