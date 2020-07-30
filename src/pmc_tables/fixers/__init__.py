import logging

import pandas as pd

import pmc_tables

from ._errors import _CouldNotApplyFixError, _FixDoesNotApplyError
from .column_dtypes import fix_column_dtypes
from .column_names import fix_column_names
from .extra_headers_and_footers import fix_extra_headers_and_footers
from .subsections import fix_subsections

logger = logging.getLogger(__name__)

HDF5_WRITER_EXCEPTIONS = (Exception)


def fix_and_write_hdf5_table(key: str, df: pd.DataFrame, store: pd.HDFStore, info: dict) -> None:
    # Easy fixes
    df = _apply_fixes(df, info, default_fixers)
    try:
        return pmc_tables.writers.write_hdf5_table(key, df, store)  # type: ignore
    except HDF5_WRITER_EXCEPTIONS as e:
        logger.warning("Encountered error `%s`", e)
        info['error_message'] = str(e)
    # More involved fixes
    df = _apply_fixes(df, info, error_fixers)
    try:
        return pmc_tables.writers.write_hdf5_table(key, df, store)  # type: ignore
    except HDF5_WRITER_EXCEPTIONS as e2:
        info['error_message_2'] = str(e2)
        raise e2


def default_fixers():
    """These fixers are applied to every DataFrame."""
    yield fix_column_names
    yield fix_subsections


def error_fixers():
    """These fixers are applied only when an error is encountered."""
    yield fix_extra_headers_and_footers
    yield fix_column_dtypes
    yield fix_column_names


def _apply_fixes(df, info, fixer_fns):
    if 'fixers_applied' not in info:
        info['fixers_applied'] = []

    for fixer_fn in fixer_fns():
        try:
            df = fixer_fn(df.copy(), info)
            info['fixers_applied'] += [fixer_fn.__name__]
        except _FixDoesNotApplyError:
            continue
        except _CouldNotApplyFixError as e2:
            logger.debug("Could not apply fix `%s` because of an error. (%s: %s)")
            continue
    return df
