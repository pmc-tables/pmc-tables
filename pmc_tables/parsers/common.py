from collections import Counter
from typing import List, Optional

import pandas as pd


def process_dataframe(df: pd.DataFrame, copy=True) -> pd.DataFrame:
    """Run the entire DataFrame processing pipeline."""
    if copy:
        df = df.copy()
    df.columns = format_columns(df.columns)
    df = add_section_index(df, copy=False)
    return df


def format_columns(columns: List[str]) -> List[str]:
    """Clean up DataFrame columns."""
    new_columns: List[str] = []
    for column_idx, column in enumerate(columns):
        if isinstance(column, (str, int)):
            new_column = _format_column(column)
        elif isinstance(column, (list, tuple)):
            new_column = ""
            for col in column:
                new_col = _format_column(col)
                if new_col:
                    new_column += f' | {new_col}' if new_col and pd.notnull(new_col) else ''
            new_column = new_column.strip('| ')
        else:
            new_column = _format_column(str(column))
        # Make sure there are no duplicates
        new_column_ref = new_column
        idx = 0
        while new_column in new_columns:
            idx += 1
            new_column = new_column_ref + '_' + str(idx)
        # Add new column
        new_columns.append(new_column)
    return new_columns


def _format_column(col: str) -> str:
    if isinstance(col, (list, tuple)):
        return ' / '.join(col)
    elif isinstance(col, str):
        if col.startswith('Unnamed:'):
            return ''
        else:
            return col
    else:
        return col


def add_section_index(df: pd.DataFrame, copy=True) -> Optional[pd.DataFrame]:
    """Treat rows which contain only a single element as a second index."""
    if copy:
        df = df.copy()
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
