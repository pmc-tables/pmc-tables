from .csv import *
from .excel import *
from .xml import *


def get_parser(name):
    if name.endswith('.xml') or name.endswith('.nxml'):
        return xml_parser
    elif name.endswith('.xls') or name.endswith('.xlsx'):
        return excel_parser
    elif any(name.endswith(ext) for ext in ['.csv', '.csv.gz', '.tsv', '.tsv.gz']):
        return csv_parser
    else:
        raise Exception(f"Do not have an extractor for file {name}")
