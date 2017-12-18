from collections import Counter

import pandas as pd

import pmc_tables

from ._errors import _FixDoesNotApplyError


def fix_subsections(df: pd.DataFrame, info: dict) -> pd.DataFrame:
    """Treat rows which contain only a single element as a second index."""
    applicable_parsers = [pmc_tables.xml_parser.__name__]
    if info['parser_name'] not in applicable_parsers:
        raise _FixDoesNotApplyError(f"This fix only applies to parsers {applicable_parsers}.")
    index_ = []
    keep_index = []
    for i, row in enumerate(df.itertuples()):
        row_items = Counter(v for v in row[1:] if pd.notnull(v))
        if len(row_items) == 1 and row_items.most_common()[0][1] > 1:
            index_.append(row_items.most_common()[0][0])
        elif i > 0:
            assert index_
            index_.append(index_[-1])
            keep_index.append(row.Index)
        else:
            return df
    df['index_'] = index_
    new_columns = ['index_'] + [c for c in df.columns if c != 'index_']
    df = df.reindex(index=keep_index, columns=new_columns)
    return df
