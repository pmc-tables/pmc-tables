class ParserError(Exception):
    """Error encountered when parsing a given file."""
    pass


class ReservedAttributeError(Exception):
    """Error raised when attempting to store a reserved attribute in an HDF5 file."""
    pass
