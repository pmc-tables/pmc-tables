import logging
import os.path as op

import pandas as pd
import tables

import pmc_tables

logger = logging.getLogger(__name__)

_RESERVED_ATTRIBUTES = [
    '_v__nodefile', '_v__nodepath', '_v_attrnames', '_v_unimplemented', '_v__format_version',
    '_v_attrnamessys', '_v_attrnamesuser', 'TITLE', 'CLASS', 'VERSION', 'pandas_type',
    'pandas_version', 'table_type', 'index_cols', 'values_cols', 'non_index_axes', 'data_columns',
    'nan_rep', 'encoding', 'levels', 'metadata', 'info'
]


def write_hdf5_table(key: str, df: pd.DataFrame, store: pd.HDFStore):
    store.put(key, df, format='table', encoding='utf-8')


def write_hdf5_metadata(key: str, value: dict, store: pd.HDFStore):
    for attr_key, attr_value in value.items():
        node = _get_or_create_node(key, store)
        if attr_key in _RESERVED_ATTRIBUTES:
            raise pmc_tables.ReservedAttributeError(attr_key)  # type: ignore
        try:
            node._f_setattr(attr_key, attr_value)
        except tables.exceptions.HDF5ExtError as e:
            logger.error("Failed setting attribute %s to %s (%s: %s).", attr_key, attr_value,
                         type(e), e)
        except AttributeError as e:
            logger.error("Failed setting attribute %s to %s (%s: %s).", attr_key, attr_value,
                         type(e), e)


def _get_or_create_node(key: str, store: pd.HDFStore) -> tables.Node:
    node = None
    # Get the node
    try:
        node = store.get_node(key)
    except tables.exceptions.NoSuchNodeError as e:
        logger.warning("Got error: %s", e)
    # Create the node
    if node is None:
        if not key.startswith('/'):
            key = '/' + key
        node = _create_node(key, store)
    assert node is not None
    return node


def _create_node(key: str, store: pd.HDFStore) -> tables.Node:
    path = op.dirname(key)
    name = op.basename(key)
    try:
        node = store._handle.create_group(path, name)
    except tables.NoSuchNodeError as e:
        if path != '/':
            _create_node(path, store)
            node = store._handle.create_group(path, name)
        else:
            raise e
    return node


def read_hdf5_table(key: str, store: pd.HDFStore) -> pd.DataFrame:
    return store.get(key)


def read_hdf5_metadata(key: str, store: pd.HDFStore) -> dict:
    node = store.get_node(key)
    metadata = {}
    for attr_key in set(node._v_attrs.__dict__.keys()) - set(_RESERVED_ATTRIBUTES):
        metadata[attr_key] = node._f_getattr(attr_key)
    return metadata
