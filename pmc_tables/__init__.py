"""Top-level package for PMC Tables."""

__author__ = """Alexey Strokach"""
__email__ = 'alex.strokach@utoronto.ca'
__version__ = '0.1.0'

from .io.html import read_html
from .ftp_client import *
from .utils import *
from .table_extraction import *
from .downloaders import *
from .xml_parsers import *
