from typing import List

import pandas as pd


def fix_column_names(df: pd.DataFrame, info: dict) -> pd.DataFrame:
    """Format all column names as strings."""
    df.columns = _format_columns(df.columns)
    assert all(isinstance(c, str) for c in df.columns)
    return df


def _format_columns(columns: List[str]) -> List[str]:
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
        return ' / '.join(str(c) for c in col)
    elif isinstance(col, str):
        if col.startswith('Unnamed:'):
            return ''
        else:
            return col
    else:
        return col
