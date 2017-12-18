from typing import List

import pytest


def parametrize(arg_string: str, data: List[dict]):
    args = [a.strip() for a in arg_string.split(',')]
    inputs = [tuple(d[arg] for arg in args) if len(args) > 1 else d[args[0]] for d in data]
    return pytest.mark.parametrize(arg_string, inputs)


def pytest_namespace():
    return {'parametrize': parametrize}
