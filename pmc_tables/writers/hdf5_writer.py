import logging
import re

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
        fixer_fns = fixer_functions()
        for fixer_fn in fixer_fns:
            result = fixer_fn(str(e), key, df, store, max_tries - 1, tried_fns)
            if result is not None:
                return result
        raise e


def fixer_functions():
    yield _fix_serialize_column_error


def _fix_serialize_column_error(error, key, df, store, max_tries, tried_fns):
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
