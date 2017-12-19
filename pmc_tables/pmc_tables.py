"""Main module."""
import json
import logging
import tempfile
import zipfile
from pathlib import Path

import pandas as pd

import pmc_tables

logger = logging.getLogger(__name__)


def save_archive_to_hdf5(archive: zipfile.ZipFile, store: pd.HDFStore) -> None:
    """Save file from an open ZIP archive into an open HDF5 file."""
    # Info
    with archive.open('info.json') as fin:
        info = json.load(fin)  # type: ignore
    info['status'] = 'success'
    pmc_id = info['pmc_id']
    logger.debug("pmc_id: `%s`", pmc_id)
    # Don't process a PMC node if it already exists
    if _node_exists(pmc_id, store):
        logger.info("PMC id `%s` already exists. Skipping...", pmc_id)
        return
    # Tables
    names = [n for n in archive.namelist() if n not in ['info.json']]
    with tempfile.TemporaryDirectory() as tmp_dir:
        for name in names:
            table_file = _extract_file(archive, name, tmp_dir)
            parser = pmc_tables.parsers.get_parser(name)
            try:
                tables = parser(table_file)
            except Exception as e:
                info['status'] = 'error'
                continue
            for table_id, table_info, table_df in tables:
                assert table_id.startswith(f'/{name}')
                # if 'table_html' in table_info:
                #     del table_info['table_html']
                key = f"/{pmc_id}{table_id}"
                table_info['status'] = 'success'
                try:
                    pmc_tables.fixers.fix_and_write_hdf5_table(key, table_df, store, table_info)
                except Exception as e:
                    info['status'] = 'error'
                    table_info['status'] = 'error'
                    table_info['final_error'] = str(type(e))
                    table_info['final_error_message'] = str(e)
                pmc_tables.writers.write_hdf5_metadata(key, table_info, store)
    pmc_tables.writers.write_hdf5_metadata(f"/{pmc_id}", info, store)


def _node_exists(pmc_id, store):
    node = None
    try:
        node = store.get_node(f'/{pmc_id}')
    except Exception:
        pass
    return node


def _extract_file(archive, name, tmp_dir):
    with archive.open(name) as fin:
        table_file = Path(tmp_dir).joinpath(name)
        with table_file.open('wb') as fout:
            fout.write(fin.read())
    return table_file
