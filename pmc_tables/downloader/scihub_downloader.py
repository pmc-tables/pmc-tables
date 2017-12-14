import os.path as op
import logging

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Access data from sci-hub
SCIHUB_BASE_URL = 'http://sci-hub.cc/'
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0'}


def download_article(doi, output_dir):
    """
    Fetches the paper by first retrieving the direct link to the pdf.
    If the indentifier is a DOI, PMID, or URL pay-wall, then use Sci-Hub
    to access and download paper. Otherwise, just download paper directly.
    """
    url = _get_scihub_url(doi)

    try:
        # verify=False is dangerous but sci-hub.io
        # requires intermediate certificates to verify
        # and requests doesn't know how to download them.
        # as a hacky fix, you can add them to your store
        # and verifying would work. will fix this later.
        res = requests.get(url, headers=HEADERS, verify=False)
    except requests.exceptions.RequestException as e:
        logger.error('Failed to fetch pdf with doi %s from url %s due to request exception!', doi,
                     url)
        return None, None,

    if res.headers['Content-Type'] != 'application/pdf':
        logger.error('Failed to fetch pdf with doi %s from url %s due to a captcha!', doi, url)
        return None, None
    else:
        filename = _slugify(doi) + '.pdf'
        if filename not in url:
            logger.warning("Filename %s not in url %s", filename, url)
        with open(op.join(output_dir, filename), 'wb') as ofh:
            ofh.write(res.content)
        return url, filename


def _slugify(doi):
    """Generate a name from DOI using the same approach as SciHub."""
    return doi.replace('/', '@')


def _get_soup(html):
    """Return html soup."""
    return BeautifulSoup(html, 'html.parser')


def _get_scihub_url(doi):
    """
    Sci-Hub embeds papers in an iframe. This function finds the actual
    source url which looks something like https://moscow.sci-hub.io/.../....pdf.
    """
    res = requests.get(SCIHUB_BASE_URL + doi, headers=HEADERS, verify=False)
    s = _get_soup(res.content)
    iframe = s.find('iframe')
    if iframe:
        if not iframe.get('src').startswith('//'):
            return iframe.get('src')
        else:
            return 'http:' + iframe.get('src')
