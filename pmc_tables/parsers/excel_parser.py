from pathlib import Path
from typing import List, Tuple

import pandas as pd


def extract_tables_from_excel(excel_file: Path) -> List[Tuple[str, dict, pd.DataFrame]]:
    sheets = pd.read_excel(excel_file.as_posix(), sheet_name=None)
    data = []
    for i, (key, df) in enumerate(sheets.items()):
        data.append((
            f"/{excel_file.name}/sheet_{i}",
            {'label': key},
            df
        ))
    return data
