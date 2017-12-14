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
    new_columns = []
    for column in columns:
        if isinstance(column, str):
            new_column = _format_column(column)
        else:
            new_column = ""
            for col in column:
                new_col = _format_column(col)
                if new_col:
                    new_column += ' / ' + new_col
            new_column = new_column.strip('/ ')
        new_columns.append(new_column)
    return new_columns


def _format_column(col: str) -> str:
    if col.startswith('Unnamed:'):
        return ''
    else:
        return col


def add_section_index(df: pd.DataFrame, copy=True) -> Optional[pd.DataFrame]:
    """Treat rows which contain only a single element as a second index."""
    if copy:
        df = df.copy()
    index_ = []
    keep_index = []
    for i, row in enumerate(df.itertuples()):
        row_items = [v for v in row[1:] if pd.notnull(v)]
        if len(set(row_items)) == 1:
            index_.append(row_items[0])
        elif i > 0:
            assert index_
            index_.append(index_[-1])
            keep_index.append(row.Index)
        else:
            return df
    df['index_'] = index_
    new_columns = [['index_'] + [c for c in df.columns if c != 'index_']]
    df = df.reindex(index=keep_index, columns=new_columns)
    return df
