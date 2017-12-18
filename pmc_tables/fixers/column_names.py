from typing import List

import pandas as pd


def fix_column_names(df: pd.DataFrame, info: dict) -> pd.DataFrame:
    """Format all column names as strings."""
    new_columns: List[str] = []
    for column_idx, column in enumerate(df.columns):
        if isinstance(column, (list, tuple)):
            new_column = ''
            for col in column:
                new_col = _format_column(col)
                if new_col and pd.notnull(new_col):
                    new_column = new_column + (' | ' if new_column else '') + new_col
        else:
            new_column = _format_column(column)
        new_column = _rename_duplicate(new_column, new_columns)
        new_columns.append(new_column)
    df.columns = new_columns
    assert all(isinstance(c, str) for c in df.columns), df.columns
    return df


def _format_column(col: object) -> str:
    col = str(col)
    if col.startswith('Unnamed:'):
        return ''
    else:
        return col


def _rename_duplicate(column: str, prev_columns: List[str]) -> str:
    column_ref = column
    idx = 0
    while column in prev_columns:
        idx += 1
        column = f'{column_ref}_{idx}'
    return column
