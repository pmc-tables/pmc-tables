import itertools
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from pmc_tables.writers import (read_hdf5_metadata, read_hdf5_table, write_hdf5_metadata,
                                write_hdf5_table)


@pytest.fixture
def store(tmpdir):
    tmpdir = Path(str(tmpdir))
    tmpfile = tempfile.NamedTemporaryFile(dir=str(tmpdir), delete=False)
    store = pd.HDFStore(tmpfile.name, mode='w')
    return store


DFS = [pd.DataFrame([[1, 1.01, 'a'], [2, 2.02, 'b']], columns=['c1', 'c2', 'c3'])]

METADATAS = [({'attr1': 100, 'attr2': 'some test'})]


@pytest.mark.parametrize("df", DFS)
def test_hdf5_table(df, store):
    write_hdf5_table('df_1', df, store)
    df_ = read_hdf5_table('df_1', store)
    assert df.equals(df_)


@pytest.mark.parametrize("metadata", METADATAS)
def test_hdf5_metadata(metadata, store):
    write_hdf5_metadata('metadata_1', metadata, store)
    metadata_ = read_hdf5_metadata('metadata_1', store)
    assert metadata == metadata_


@pytest.mark.parametrize("df, metadata", itertools.product(DFS, METADATAS))
def test_hdf5_table_and_metadata_1(df, metadata, store):
    write_hdf5_table('df_1', df, store)
    write_hdf5_metadata('metadata_1', metadata, store)
    df_ = read_hdf5_table('df_1', store)
    assert df.equals(df_)
    metadata_ = read_hdf5_metadata('metadata_1', store)
    assert metadata == metadata_


@pytest.mark.parametrize("df, metadata", itertools.product(DFS, METADATAS))
def test_hdf5_table_and_metadata_2(df, metadata, store):
    write_hdf5_metadata('metadata_1', metadata, store)
    write_hdf5_table('df_1', df, store)
    metadata_ = read_hdf5_metadata('metadata_1', store)
    assert metadata == metadata_
    df_ = read_hdf5_table('df_1', store)
    assert df.equals(df_)
