import os.path as op
import tempfile
import zipfile
from pathlib import Path

import pytest

import pmc_tables

BASE_PATH = Path(op.splitext(op.abspath(__file__))[0])

TEST_FILES = [
    # (source_url, source_file, output_file)
    ('ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_package/02/f1/PMC2203987.tar.gz',
     'source/oa_package/02/f1/PMC2203987.tar.gz', 'output/00/0a/PMC2203987.zip')
]


def compare_archive_contents(archive1, archive2):
    archive1_contents = zipfile.ZipFile(archive1).namelist()
    archive2_contents = zipfile.ZipFile(archive2).namelist()
    assert archive1_contents == archive2_contents


@pytest.mark.parametrize('source_url, source_file, output_file', TEST_FILES)
def test_get_pmc_archive_1(source_url, source_file, output_file):
    with tempfile.NamedTemporaryFile(suffix='.zip') as zip_file:
        pmc_tables.get_pmc_archive(source_url, zip_file.name)
        compare_archive_contents(BASE_PATH.joinpath(output_file), zip_file.name)


@pytest.mark.parametrize('source_url, source_file, output_file', TEST_FILES)
def test_get_pmc_archive_2(source_url, source_file, output_file):
    with tempfile.NamedTemporaryFile(suffix='.zip') as zip_file:
        pmc_tables.get_pmc_archive(BASE_PATH.joinpath(source_file).as_posix(), zip_file.name)
        compare_archive_contents(BASE_PATH.joinpath(output_file), zip_file.name)
