"""Top-level package for PMC Tables."""

__author__ = """Alexey Strokach"""
__email__ = 'alex.strokach@utoronto.ca'
__version__ = '0.1.0'

from .io.html import read_html
from .utils import *
from .ftp_client import get_ftp_client
from .downloader import get_pmc_archive
from .table_extraction import *
from .pmc_xml_parser import *
from .pubmed_xml_parser import *
