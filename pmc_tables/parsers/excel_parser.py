from pathlib import Path

import pandas as pd


def read_excel(filepath: Path) -> pd.DataFrame:
    sheets = pd.read_excel(filepath.as_posix(), sheet_name=None)
    data = {}
    for i, (key, df) in enumerate(sheets.items()):
        unique = f"/{filepath.name}/sheet_{i}"
        data[unique] = {
            'label': key,
            'table_df': df,
        }
    return data
