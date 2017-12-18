import pmc_tables


def get_parser(name):
    if name.endswith('.xml') or name.endswith('.nxml'):
        return pmc_tables.xml_parser
    elif name.endswith('.xls') or name.endswith('.xlsx'):
        return pmc_tables.excel_parser
    elif any(name.endswith(ext) for ext in ['.csv', '.csv.gz', '.tsv', '.tsv.gz']):
        return pmc_tables.csv_parser
    else:
        raise Exception(f"Do not have an extractor for file {name}")
