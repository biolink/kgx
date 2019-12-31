import re
import stringcase
from bmt import Toolkit
from cachetools import LRUCache
from prefixcommons import contract_uri
from prefixcommons.curie_util import default_curie_maps

toolkit = None
curie_lookup_service = None
cache = None


cmaps = [
            {
                'OMIM': 'https://omim.org/entry/',
                'HGNC': 'http://identifiers.org/hgnc/',
                'DRUGBANK': 'http://identifiers.org/drugbank:',
                'biolink': 'http://w3id.org/biolink/vocab/'
            },
            {
                'DRUGBANK': 'http://w3id.org/data2services/data/drugbank/'
            }
        ] + default_curie_maps


def camelcase_to_sentencecase(s: str) -> str:
    """
    Convert CamelCase to sentence case.

    Parameters
    ----------
    s: str
        Input string in CamelCase

    Returns
    -------
    str
        a normal string

    """
    return stringcase.sentencecase(s).lower()

def snakecase_to_sentencecase(s: str) -> str:
    """
    Convert snake_case to sentence case.

    Parameters
    ----------
    s: str
        Input string in snake_case

    Returns
    -------
    str
        a normal string

    """
    return stringcase.sentencecase(s).lower()

def sentencecase_to_snakecase(s: str) -> str:
    """
    Convert sentence case to snake_case.

    Parameters
    ----------
    s: str
        Input string in sentence case

    Returns
    -------
    str
        a normal string

    """
    return stringcase.snakecase(s).lower()


def contract(uri) -> str:
    """
    We sort the curies to ensure that we take the same item every time
    """
    curies = contract_uri(str(uri), cmaps=cmaps)
    if len(curies) > 0:
        curies.sort()
        return curies[0]
    return None

def make_curie(uri) -> str:
    HTTP = 'http'
    HTTPS = 'https'

    curie = contract(uri)

    if curie is not None:
        return curie

    if uri.startswith(HTTPS):
        uri = HTTP + uri[len(HTTPS):]
    elif uri.startswith(HTTP):
        uri = HTTPS + uri[len(HTTP):]

    curie = contract(uri)

    if curie is None:
        return uri
    else:
        return curie

def get_toolkit() -> Toolkit:
    """
    Get an instance of bmt.Toolkit
    If there no instance defined, then one is instantiated and returned.

    Returns
    -------
    bmt.Toolkit
        an instance of bmt.Toolkit

    """
    global toolkit
    if toolkit is None:
        toolkit = Toolkit()

    return toolkit

def generate_edge_key(s: str, edge_label: str, o: str) -> str:
    """
    Generates an edge key based on a given subject, edge_label and object.

    Parameters
    ----------
    s: str
        Subject
    edge_label: str
        Edge label
    o: str
        Object

    Returns
    -------
    str
        Edge key as a string

    """
    return '{}-{}-{}'.format(s, edge_label, o)

def get_biolink_mapping(category):
    global toolkit
    element = toolkit.get_element(category)
    if element is None:
        element = toolkit.get_element(snakecase_to_sentencecase(category))
    return element

def get_curie_lookup_service():
    global curie_lookup_service
    if curie_lookup_service is None:
        from kgx.curie_lookup_service import CurieLookupService
        curie_lookup_service = CurieLookupService()
    return curie_lookup_service

# TODO: To be removed
def get_curie_lookup_map():
    global curie_lookup_service
    from kgx.curie_lookup_service import CurieLookupService
    if curie_lookup_service is None:
        curie_lookup_service = CurieLookupService()
    return curie_lookup_service.curie_map

def get_cache(maxsize=10000):
    global cache
    if cache is None:
        cache = LRUCache(maxsize)
    return cache
