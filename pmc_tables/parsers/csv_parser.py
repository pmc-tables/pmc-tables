from pathlib import Path

import pandas as pd


def read_csv(filepath: Path) -> pd.DataFrame:
    unique = f"/{filepath.name}/sheet_0"
    df = None
    for sep in [',', '\t']:
        try:
            df = pd.read_csv(filepath.as_posix(), sep=sep)
        except (pd.errors.ParserError, UnicodeDecodeError):
            continue
        if len(df.columns) > 1:
            break
    data = {unique: {'label': None, 'table_df': df}}

    return data
