"""
Downlaod data from PubMed Central.
"""
import datetime
import json
import logging
import os
import os.path as op
import shutil
import tarfile
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Dict, List

from pmc_tables.utils import recursive_listdir

logger = logging.getLogger(__name__)

EXTENSIONS_TO_KEEP = ['.nxml', '.xml', '.csv', '.csv.gz', '.tsv', '.tsv.gz', '.xls', '.xlsx']


def get_pmc_archive(archive_file: str, output_file: str, global_tmp_dir: str = '/dev/shm',
                    **kwargs) -> Dict[str, str]:
    archive_path = _get_archive(archive_file, global_tmp_dir)

    info = {
        **kwargs,
        'created_on': datetime.datetime.now().isoformat(),
        'archive_file': archive_file,
    }

    with tempfile.TemporaryDirectory(dir=global_tmp_dir) as tmp_dir:
        _create_archive_subset(archive_path, Path(output_file), Path(tmp_dir), info)

    return info


def _get_archive(archive_file: str, global_tmp_dir: str) -> Path:
    """Return a `Path` object for the `archive_file`.

    If `archive_file` is a URL, download the file first.
    """
    if archive_file.startswith('ftp://'):
        suffix = op.basename(archive_file).partition('.')[-1]
        archive_file_obj = tempfile.NamedTemporaryFile(dir=global_tmp_dir, suffix=f'.{suffix}')
        archive_path = Path(archive_file_obj.name)
        urllib.request.urlretrieve(archive_file, archive_path.as_posix())
    elif op.isfile(archive_file):
        archive_path = Path(archive_file)
    else:
        raise FileNotFoundError(archive_file)
    return archive_path


def _create_archive_subset(archive_path: Path, output_path: Path, tmp_path: Path, info: dict):
    """Create a new archive which contains only a subset of files from the input archive.
    """
    _extract_archive(archive_path, tmp_path)

    all_files = recursive_listdir(tmp_path)
    removed_files = _remove_unkept_files(all_files)
    kept_files = [f for f in all_files if f not in removed_files and f.is_file()]

    info.update({
        'all_files': [f.relative_to(tmp_path).as_posix() for f in all_files],
        'kept_files': [f.relative_to(tmp_path).as_posix() for f in kept_files],
    })
    kept_files.append(_write_info_file(info, tmp_path))

    output_path.mkdir(parents=True, exist_ok=True)
    _write_to_archive(kept_files, output_path)


def _extract_archive(archive_path: Path, tmp_path: Path):
    if '.tar' in archive_path.suffixes or '.zip' in archive_path.suffixes:
        shutil.unpack_archive(archive_path.as_posix(), extract_dir=tmp_path)
    elif archive_path.suffix == '.xml':
        shutil.copy(archive_path, tmp_path.joinpath(archive_path.name))
    else:
        raise Exception(f"Unsupported archive: {archive_path}")


def _remove_unkept_files(file_list: List[Path],
                         extensions_to_keep: List[str] = EXTENSIONS_TO_KEEP) -> List[Path]:
    """Remove files which do not match the extensions in `extensions_to_keep`."""
    removed_files = []
    for filepath in file_list:
        if not op.isfile(filepath):
            continue
        elif not any(filepath.name.endswith(ext) for ext in extensions_to_keep):
            os.remove(filepath)
            removed_files.append(filepath)
    return removed_files


def _write_info_file(info: dict, tmp_path: Path) -> Path:
    info_file = tmp_path.joinpath('info.json')
    with open(info_file, 'wt') as fout:
        json.dump(info, fout)
    return info_file


def _write_to_archive(file_list: List[Path], output_file: Path) -> None:
    """Create a *.tar.gz or *.zip archive from a list of files."""
    if '.tar' in output_file.suffixes:
        with tarfile.open(output_file, 'w:gz') as tar_file:
            for file in file_list:
                tar_file.add(file.as_posix(), file.name)
    elif '.zip' in output_file.suffixes:
        with zipfile.ZipFile(
                output_file.as_posix(), 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
            for file in file_list:
                zip_file.write(file.as_posix(), file.name)
    else:
        raise Exception("Unsupported archive format!")
