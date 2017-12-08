"""
Downlaod data from PubMed Central.
"""
import datetime
import logging
import os
import os.path as op
import shutil
import tempfile
import urllib.request
import json
import zipfile
import tarfile
from ftplib import FTP
from typing import Dict, List

logger = logging.getLogger(__name__)

NCBI_FTP_URL = 'ftp://ftp.ncbi.nlm.nih.gov'
EBI_FTP_URL = 'ftp.ebi.ac.uk'
EBI_PMC_PATH = '/pub/databases/pmc'

EXTENSIONS_TO_KEEP = ['.nxml', '.xml', '.csv', '.csv.gz', '.tsv', '.tsv.gz', '.xls', '.xlsx']


def get_ftp_client(host: str) -> FTP:
    """Get FTP client for NCBI or EBI."""
    assert host in ['ncbi', 'ebi']
    if host == 'ncbi':
        ftp = FTP(NCBI_FTP_URL)
        ftp.sendcmd('USER anonymous')
        ftp.sendcmd('PASS anonymous')
    elif host == 'ebi':
        ftp = FTP(EBI_FTP_URL)
        ftp.login()
    return ftp


def download_archive(archive_path: str,
                     output_file: str,
                     global_tmp_dir: str = '/dev/shm',
                     **kwargs) -> Dict[str, str]:
    # info
    info = kwargs
    info['created_on'] = datetime.datetime.now().isoformat()

    # archive_file
    if archive_path.startswith('ftp://'):
        suffix = op.basename(archive_path).partition('.')[-1]
        archive_file_obj = tempfile.NamedTemporaryFile(dir=global_tmp_dir, suffix=f'.{suffix}')
        archive_file = archive_file_obj.name
        urllib.request.urlretrieve(archive_path, archive_file)
    elif not op.isfile(archive_path):
        raise FileNotFoundError(archive_path)
    else:
        archive_file = archive_path
    info['archive_file'] = archive_file

    # Create new zip archive
    with tempfile.TemporaryDirectory(dir=global_tmp_dir) as tmp_dir:
        if archive_file.endswith('.tar.gz') or archive_file.endswith('.zip'):
            shutil.unpack_archive(archive_file, extract_dir=tmp_dir)
        elif archive_file.endswith('.xml'):
            shutil.copy(archive_file, op.join(tmp_dir, op.basename(archive_file)))
        else:
            raise Exception()

        all_files = recursive_listdir(tmp_dir)
        removed_files = remove_unkept_files(all_files)
        kept_files = [f for f in all_files if f not in removed_files and op.isfile(f)]
        info['all_files'] = [op.relpath(f, tmp_dir) for f in all_files]
        info['kept_files'] = [op.relpath(f, tmp_dir) for f in kept_files]

        info_file = op.join(tmp_dir, 'info.json')
        with open(info_file, 'wt') as fout:
            json.dump(info, fout)
        kept_files.append(info_file)

        os.makedirs(op.dirname(output_file), exist_ok=True)
        write_to_archive(kept_files, output_file)
    return info


def write_to_archive(file_list: List[str], output_file: str) -> None:
    if output_file.endswith('.tar.gz'):
        with tarfile.open(output_file, 'w:gz') as tar_file:
            for file in file_list:
                tar_file.add(file, op.basename(file))
    elif output_file.endswith('.zip'):
        with zipfile.ZipFile(output_file, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
            for file in file_list:
                zip_file.write(file, op.basename(file))
    else:
        raise Exception("Unsupported archive format!")


def recursive_listdir(path):
    return [op.join(dp, f) for dp, dn, fn in os.walk(path) for f in fn]


def remove_unkept_files(file_list) -> List[str]:
    removed_files = []
    for filepath in file_list:
        if not op.isfile(filepath):
            continue
        elif not any(filepath.endswith(ext) for ext in EXTENSIONS_TO_KEEP):
            os.remove(filepath)
            removed_files.append(filepath)
    return removed_files
