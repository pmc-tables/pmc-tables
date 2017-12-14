import os
from pathlib import Path
from typing import List, Union


def recursive_listdir(path: Union[str, Path]) -> List[Path]:
    """Like `os.listdir`, but recursive."""
    return [Path(dp).joinpath(f) for dp, dn, fn in os.walk(path) for f in fn]
