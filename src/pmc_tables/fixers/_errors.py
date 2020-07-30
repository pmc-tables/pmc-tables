class _FixDoesNotApplyError(Exception):
    """Error raised when a given fixer function does not apply to a particular case."""
    pass


class _CouldNotApplyFixError(Exception):
    """Error raised when we could not apply a fix for some reason."""
    pass
