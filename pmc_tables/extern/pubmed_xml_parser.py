"""
PubMed XML Parser
-----------------

This module contains functions for parsing XML files created using
the PubMed Send to: File (XML) option.
"""
import xml.etree.ElementTree as ET
from typing import List, NamedTuple

import smart_open


class PubmedRow(NamedTuple):
    pmid: str
    title: str
    authors: str
    journal: str
    year_published: str
    abstract: str
    mesh_terms: str
    doi: str
    pmc: str


def parse_pubmed_xml_file(xml_file: str) -> List[PubmedRow]:
    """Parse PubMed XML File.

    This function works both for files downloaded using the "Send to:" option
    on the PubMed website and files available from the NCBI FTP site.
    """
    data = []
    for child in _iter_root_children(xml_file):
        row = _process_child(child)
        data.append(row)
    return data


def _iter_root_children(xml_file):
    with smart_open.smart_open(xml_file) as ifh:
        tree = ET.parse(ifh)
    root = tree.getroot()
    yield from root


def _process_child(child):
    row = PubmedRow(
        _find_pmid(child),
        _find_title(child),
        _find_authors(child),
        _find_journal(child),
        _find_year_published(child),
        _find_abstract(child),
        _find_mesh_terms(child),
        _find_doi(child),
        _find_pmc(child),)
    return row


def _get_text_attr(attr):
    if attr is None:
        return None
    elif not hasattr(attr, 'text') or attr.text is None:
        return None
    else:
        return attr.text.strip()


def _find_pmid(child) -> int:
    pmid = child.find('MedlineCitation/PMID')
    pmid_text = _get_text_attr(pmid)
    return int(pmid_text) if pmid_text else None


def _find_title(child) -> str:
    title = child.find('MedlineCitation/Article/ArticleTitle')
    return _get_text_attr(title)


def _find_authors(child):
    authors = child.findall('MedlineCitation/Article/AuthorList/Author/LastName')
    return [_get_text_attr(a) for a in authors]


def _find_journal(child):
    journal = child.find('MedlineCitation/MedlineJournalInfo/MedlineTA')
    return _get_text_attr(journal)


def _find_year_published(child):
    year_published = child.find('MedlineCitation/Article/Journal/JournalIssue/PubDate/Year')
    year_published_text = _get_text_attr(year_published)
    return int(year_published_text) if year_published_text else None


def _find_abstract(child) -> str:
    abstract = child.find('MedlineCitation/Article/Abstract/AbstractText')
    return _get_text_attr(abstract)


def _find_mesh_terms(child):
    mesh_terms = child.findall('MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName')
    return [_get_text_attr(v) for v in mesh_terms]


def _find_doi(child) -> str:
    ids = child.findall('PubmedData/ArticleIdList/ArticleId')
    ids = [id_ for id_ in ids if id_.attrib['IdType'] == 'doi']
    return _get_text_attr(ids[0]) if ids else None


def _find_pmc(child) -> str:
    ids = child.findall('PubmedData/ArticleIdList/ArticleId')
    ids = [id_ for id_ in ids if id_.attrib['IdType'] == 'pmc']
    return _get_text_attr(ids[0]) if ids else None
