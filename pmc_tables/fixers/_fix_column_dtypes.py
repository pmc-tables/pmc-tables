from ._common import _format_mixed_columns
from ._errors import _FixDoesNotApplyError


def fix_column_dtypes(df, error):
    if error and not error.startswith("Cannot serialize the column"):
        raise _FixDoesNotApplyError(f"Unsupported error message: {error}.")
    df = _format_mixed_columns(df)
    return df
