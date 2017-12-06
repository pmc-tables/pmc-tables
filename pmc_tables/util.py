from typing import List

import pandas as pd


def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = format_columns(df)
    df = add_section_index(df)
    return df


def format_columns(df: pd.DataFrame) -> List[str]:
    new_columns = []
    for column in df.columns:
        if isinstance(column, str):
            new_column = prettify_column(column)
        else:
            new_column = ""
            for col in column:
                new_col = prettify_column(col)
                if new_col:
                    new_column += ' / ' + new_col
            new_column = new_column.strip('/ ')
        new_columns.append(new_column)
    return new_columns


def prettify_column(col: str) -> str:
    if col.startswith('Unnamed:'):
        return ''
    else:
        return col


def add_section_index(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    index_ = []
    skiprows = []
    for i, row in enumerate(df.itertuples()):
        row_items = [v for v in row[1:] if pd.notnull(v)]
        if len(set(row_items)) == 1:
            index_.append(row_items[0])
            skiprows.append(row.Index)
        elif i > 0:
            assert index_
            index_.append(index_[-1])
        else:
            return df
    df['index_'] = index_
    df = df.drop(pd.Index(skiprows))
    df = df[['index_'] + [c for c in df.columns if c != 'index_']]
    return df
