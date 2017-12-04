import os.path as op
import pickle
from typing import Dict

import tqdm

import pmc_tables


def get_ebi_pcm_suppl_url_mapping():
    ftp = pmc_tables.get_ftp_client('ebi')
    folders = []
    for subset in ['NON-OA', 'OA']:
        folders = ftp.nlst(f'/pub/databases/pmc/suppl/{subset}/')
        for folder1 in tqdm.tqdm(folders, total=len(folders)):
            for folder2 in ftp.nlst(folder1):
                folders.append(folder1)
    return folders


if __name__ == '__main__':
    folders = get_ebi_pcm_suppl_url_mapping()
    mapping: Dict[str, str] = {}
    for folder in folders:
        pmc_id = op.basename(folder).split('.')[0]
        assert pmc_id not in mapping
        mapping[pmc_id] = folder
    with open('ebi_pmc_suppl_mapping.pickle', 'wb') as fout:
        pickle.dump(mapping, fout, pickle.HIGHEST_PROTOCOL)
