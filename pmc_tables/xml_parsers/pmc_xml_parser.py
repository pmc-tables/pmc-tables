"""
XML Table Parser
----------------

Extract tables found in PubMed Central XML files.
"""
import xml.etree.ElementTree as ET
import re

count = 0


def get_tables(root):
    table_wraps_all = []
    tables_all = []
    for child in root:
        table_wraps = child.findall('*/table-wrap')
        tables = child.findall('**/table')
        assert len(table_wraps) == len(tables), (len(table_wraps), len(tables))
        table_wraps_all.extend(table_wraps)
        tables_all.extend(tables)
    return table_wraps_all, tables_all


def caption_to_string(element):
    if element is None:
        return ""
    element_text = ET.tostring(element, encoding='utf8', method='text').decode('utf-8')
    element_text = element_text.strip().replace('\n', ' ')
    element_text = re.sub(' +', ' ', element_text)
    return element_text


def extract_tables_from_xml(xml_file):
    tree = ET.parse(xml_file)
    tables = tree.findall('.//table')
    table_wraps = tree.findall('.//table-wrap')

    data = {}

    for table_wrap in table_wraps:
        # Id
        id_ = table_wrap.get('id')
        assert id_ is not None
        logger.debug(f"  id: {id_}")
        # Attrib
        attrib_set = set(table_wrap.attrib)
        if attrib_set - {'id', 'position'}:
            print(f"tw({id_}): {attrib_set}")
        # Label
        label = table_wrap.find('label')
        label_text = label.text if label is not None else ""
        logger.debug(f"  label_text: {label_text}")
        # Caption
        caption = table_wrap.find('caption')
        caption_text = caption_to_string(caption)
        logger.debug(f"  caption_text: {caption_text}")
        # Footer
        footer = table_wrap.find('table-wrap-foot')
        footer_text = caption_to_string(footer)
        logger.debug(f"  footer_text: {footer_text}")
        # Other children
        other_children = [
            c.tag for c in table_wrap.getchildren()
            if c.tag not in ['label', 'caption', 'table', 'table-wrap-foot']
        ]
        if other_children:
            logger.info(other_children)
        # Tables
        tables = table_wrap.findall('table')
        for i, table in enumerate(tables):
            unique = f"{op.basename(row.File)}::{id_}-{i}"
            assert unique not in results
            logger.debug(f"    unique: {unique}")
            # Attrib
            attrib_set = {a for a in table.attrib if a not in ['frame', 'rules']}
            if attrib_set:
                logger.info(attrib_set)
            # Children
            children_set = {c.tag for c in table.getchildren() if c.tag not in ['thead', 'tbody']}
            if children_set:
                logger.info(children_set)
            # DataFrame
            table_bytes = ET.tostring(table)
            # display(HTML(table_bytes.decode('utf-8')))
            # table_dfs = pd.read_html(table_bytes, na_values=['', '-', '–'])
            # assert len(table_dfs) == 1
            # table_df = table_dfs[0]
            # display(table_df)
            #
            data[unique] = (table_df_dev, {
                'id': id_,
                'file_name': row.File,

            })
            try:
                table_df_dev = pmc_tables.read_html(table_bytes, na_values=['', '-', '–'])[0]
                table_df_dev = pmc_tables.process_dataframe(table_df_dev)
                # display(table_df_dev)
                store.put('/' + unique, table_df_dev)
            except:
                pass
