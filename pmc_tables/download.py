"""
Downlaod data from PubMed Central.

This is done in Jupyter notebooks right now...
"""
import logging
import os
import os.path as op
import urllib.request
from ftplib import FTP
from typing import List, Tuple, Optional
from zipfile import ZipFile

logger = logging.getLogger(__name__)

NCBI_FTP_URL = 'ftp://ftp.ncbi.nlm.nih.gov'
EBI_FTP_URL = 'ftp.ebi.ac.uk'
EBI_PMC_PATH = '/pub/databases/pmc'


def get_ftp_client(host: str) -> FTP:
    if host == 'ncbi':
        ftp = FTP(NCBI_FTP_URL)
        ftp.sendcmd('USER anonymous')
        ftp.sendcmd('PASS anonymous')
    elif host == 'ebi':
        ftp = FTP(EBI_FTP_URL)
        ftp.login()
    else:
        raise Exception(f"Unknown host: {host}.")
    return ftp


def get_ebi_suppl_folder_list(ftp: FTP) -> Tuple[List[str], List[str]]:
    ftp.cwd(f"{EBI_PMC_PATH}/suppl/OA/")
    oa_folders = ftp.nlst()
    ftp.cwd(f"{EBI_PMC_PATH}/suppl/NON-OA/")
    non_oa_folders = ftp.nlst()
    return oa_folders, non_oa_folders


def get_containing_folder(pmc_id: str, folders: List[str]) -> str:
    for folder in folders:
        idx = int(pmc_id[3:])
        start, end = (int(f[3:]) for f in folder.split('-'))
        if start <= idx <= end:
            return folder
    raise Exception(f"Could not find containing folder for PMC: {pmc_id}!")


class EbiDownloader:

    source_urls = [
        f"ftp://{EBI_FTP_URL}{EBI_PMC_PATH}/suppl",
    ]

    def __init__(self, archive_dir=None):
        self.ftp = get_ftp_client('ebi')
        if archive_dir is None:
            self.archive_dir = op.join(os.getcwd(), '.pmc')
        else:
            self.archive_dir = archive_dir
        self.oa_folders, self.non_oa_folders = self._get_listdir()

    def download_ebi_suppl(self, pmc_id: str) -> Optional[str]:
        for subset in ['OA', 'NON-OA']:
            containing_folder = get_containing_folder(pmc_id, self.oa_folders
                                                      if subset == 'OA' else self.non_oa_folders)

            output = op.join(self.archive_dir, 'suppl', subset, containing_folder, f"{pmc_id}.zip")
            if op.isfile(output):
                logger.debug("File %s already exists.", output)
                return output
            os.makedirs(op.dirname(output), exist_ok=True)

            for source_url in self.source_urls:
                url = f"{source_url}/{subset}/{containing_folder}/{pmc_id}.zip"
                try:
                    filename, headers = urllib.request.urlretrieve(url, output)
                except (ValueError, urllib.request.URLError) as e:
                    logger.debug("Could not download file %s.\n%s.", url, e)
                    continue
                return filename

        logger.error(f"Could not download suppl file for {pmc_id}!")
        return None

    def _get_listdir(self):
        oa_listdir_file = op.join(self.archive_dir, 'oa.listdir')
        non_oa_listdir_file = op.join(self.archive_dir, 'non-oa.listdir')
        if op.isfile(oa_listdir_file) and op.isfile(non_oa_listdir_file):
            with open(oa_listdir_file) as fin:
                oa_folders = [line.strip() for line in fin if line.strip()]
            with open(non_oa_listdir_file) as fin:
                non_oa_folders = [line.strip() for line in fin if line.strip()]
        else:
            oa_folders, non_oa_folders = get_ebi_suppl_folder_list(self.ftp)
            with open(oa_listdir_file, 'wt') as fout:
                fout.write('\n'.join(oa_folders))
            with open(non_oa_listdir_file, 'wt') as fout:
                fout.write('\n'.join(non_oa_folders))
        return oa_folders, non_oa_folders
