"""
XML Table Parser
----------------

Extract tables found in PubMed Central XML files.
"""
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Tuple

import pandas as pd

import pmc_tables

from ._pandas.io.html import read_html

logger = logging.getLogger(__name__)


class TableWrapRow(NamedTuple):
    id_: str
    label: str
    caption: str
    footer: str


def read_xml(table: bytes) -> pd.DataFrame:
    table_df = read_html(table)
    assert len(table_df) == 1
    return table_df[0]


def extract_tables_from_xml(xml_file: Path) -> dict:
    tree = ET.parse(xml_file.as_posix())
    table_wraps = tree.findall('.//table-wrap')
    num_tables = len(tree.findall('.//table'))

    data: Dict[str, Any] = {}
    for table_wrap in table_wraps:
        tw_row, tables = _process_table_wrap(table_wrap)
        for i, table in enumerate(tables):
            unique = f"/{xml_file.name}/{tw_row.id_}-{i}"
            table_bytes = _process_table(table)
            table_df = pmc_tables.read_html(table_bytes)
            data[unique] = {
                **tw_row._asdict(),
                'table_html': pmc_tables.compress_to_b85(table_bytes).decode('ascii'),
                'table_df': table_df,
            }
    assert len(data) == num_tables
    return data


def _process_table_wrap(table_wrap: ET.Element) -> Tuple[TableWrapRow, List[ET.Element]]:
    # Id
    id_ = table_wrap.get('id')
    assert id_ is not None
    # Attrib
    attrib_set = set(table_wrap.attrib)
    assert not attrib_set - {'id', 'position'}, attrib_set
    # Label
    label = table_wrap.find('label')
    label_text = label.text if label is not None else ""
    # Caption
    caption = table_wrap.find('caption')
    caption_text = caption_to_string(caption)
    # Tables
    tables = table_wrap.findall('table')
    # Footer
    footer = table_wrap.find('table-wrap-foot')
    footer_text = caption_to_string(footer)
    # Other children
    children_set = {e.tag for e in table_wrap.getchildren()}
    assert not children_set - {'label', 'caption', 'table', 'table-wrap-foot'}, children_set
    # Return
    return TableWrapRow(id_, label_text, caption_text, footer_text), tables


def caption_to_string(element: ET.Element) -> str:
    if element is None:
        return ""
    element_text = ET.tostring(element, encoding='utf8', method='text').decode('utf-8')
    element_text = element_text.strip().replace('\n', ' ')
    element_text = re.sub(' +', ' ', element_text)
    return element_text


def _process_table(table: ET.Element) -> bytes:
    # Attrib
    attrib_set = set(table.attrib)
    assert not attrib_set - {'frame', 'rules'}, attrib_set
    # Children
    children_set = {e.tag for e in table.getchildren()}
    assert not children_set - {'thead', 'tbody'}, children_set
    # DataFrame
    table_bytes = ET.tostring(table)
    return table_bytes
