import os
from pathlib import Path
from typing import List, Union

import pyarrow as pa


def recursive_listdir(path: Union[str, Path]) -> List[Path]:
    """Like `os.listdir`, but recursive."""
    return [Path(dp).joinpath(f) for dp, dn, fn in os.walk(path) for f in fn]


def df_to_arrow(df, integer_columns=None, integer_dtypes=None):
    """
    Not sure what this does yet...
    """
    extra_columns = {}
    for column, dtype in zip(integer_columns, integer_dtypes):
        extra_columns[column] = {
            'dtype': dtype,
            'idx': list(df.columns).index(column),
            'data': df[column]
        }

    table = pa.Table.from_pandas(
        df[[c for c in df.columns if c not in integer_columns]], preserve_index=False)

    for column_name, column_attrib in sorted(extra_columns.items(), key=lambda c: c[1]['idx']):
        array = pa.Array.from_pandas(column_attrib['data'], column_attrib['data'].isnull(),
                                     column_attrib['dtype'])
        column = pa.Column.from_array(column_name, array)
        table = table.add_column(column_attrib['idx'], column)

    return table
