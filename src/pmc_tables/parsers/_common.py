import functools
from pathlib import Path
from typing import Callable, List, Tuple

import pandas as pd


def parser(fn: Callable):

    @functools.wraps(fn)
    def wraped(filepath: Path) -> List[Tuple[str, dict, pd.DataFrame]]:
        results = fn(filepath)
        for result in results:
            assert 'label' in result[1]
            result[1]['parser_name'] = fn.__name__
        return results

    return wraped
