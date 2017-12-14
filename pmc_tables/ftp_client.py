from ftplib import FTP

NCBI_FTP_URL = 'ftp://ftp.ncbi.nlm.nih.gov'
EBI_FTP_URL = 'ftp.ebi.ac.uk'
EBI_PMC_PATH = '/pub/databases/pmc'


def get_ftp_client(host: str) -> FTP:
    """Return the FTP client for NCBI or EBI servers."""
    assert host in ['ncbi', 'ebi']
    if host == 'ncbi':
        ftp = FTP(NCBI_FTP_URL)
        ftp.sendcmd('USER anonymous')
        ftp.sendcmd('PASS anonymous')
    elif host == 'ebi':
        ftp = FTP(EBI_FTP_URL)
        ftp.login()
    return ftp
