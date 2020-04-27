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
    Contract a URI a CURIE.
    We sort the curies to ensure that we take the same item every time.

    Parameters
    ----------
    uri: Union[rdflib.term.URIRef, str]
        A URI

    Returns
    -------
    str
        The CURIE

    """
    curies = contract_uri(str(uri), cmaps=cmaps)
    if len(curies) > 0:
        curies.sort()
        return curies[0]
    return None

def make_curie(uri) -> str:
    """
    # TODO: get rid of this method
    Convert a given URI into a CURIE.
    This method tries to handle the ``http`` and ``https``
    ambiguity in URI contraction.

    .. warning::
        This is a temporary solution and will be deprecated in the near future.

    """
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
        return str(uri)
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
    """
    Get a BioLink Model mapping for a given ``category``.

    Parameters
    ----------
    category: str
        A category for which there is a mapping in BioLink Model

    Returns
    -------
    str
        A BioLink Model class corresponding to ``category``

    """
    global toolkit
    element = toolkit.get_element(category)
    if element is None:
        element = toolkit.get_element(snakecase_to_sentencecase(category))
    return element

def get_curie_lookup_service():
    """
    Get an instance of kgx.curie_lookup_service.CurieLookupService

    Returns
    -------
    kgx.curie_lookup_service.CurieLookupService
        An instance of ``CurieLookupService``

    """
    global curie_lookup_service
    if curie_lookup_service is None:
        from kgx.curie_lookup_service import CurieLookupService
        curie_lookup_service = CurieLookupService()
    return curie_lookup_service

def get_cache(maxsize=10000):
    """
    Get an instance of cachetools.cache

    Parameters
    ----------
    maxsize: int
        The max size for the cache (``10000``, by default)

    Returns
    -------
    cachetools.cache
        An instance of cachetools.cache

    """
    global cache
    if cache is None:
        cache = LRUCache(maxsize)
    return cache
