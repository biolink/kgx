import pytest
from bmt import Toolkit

from kgx.curie_lookup_service import CurieLookupService
from kgx.utils.kgx_utils import get_toolkit, get_curie_lookup_service, get_prefix_prioritization_map, \
    get_biolink_element, get_biolink_ancestors, generate_edge_key, contract, expand, camelcase_to_sentencecase, \
    snakecase_to_sentencecase, sentencecase_to_snakecase, sentencecase_to_camelcase, generate_uuid


def test_get_toolkit():
    tk = get_toolkit()
    assert isinstance(tk, Toolkit)


def test_get_curie_lookup_service():
    cls = get_curie_lookup_service()
    assert isinstance(cls, CurieLookupService)


def test_get_prefix_prioritization_map():
    prioritization_map = get_prefix_prioritization_map()
    assert 'biolink:Gene' in prioritization_map.keys()
    assert 'biolink:Protein' in prioritization_map.keys()
    assert 'biolink:Disease' in prioritization_map.keys()


def test_get_biolink_element():
    # TODO: Parameterize
    element1 = get_biolink_element('gene')
    assert element1 is not None
    assert element1.name == 'gene'

    element2 = get_biolink_element('biolink:Gene')
    assert element2 is not None
    assert element2 == element1


def test_get_biolink_ancestors():
    # TODO: Parameterize
    ancestors1 = get_biolink_ancestors('phenotypic feature')
    assert ancestors1 is not None
    assert len(ancestors1) == 4


def test_generate_edge_key():
    key = generate_edge_key('S:CURIE', 'related_to', 'O:CURIE')
    assert key == 'S:CURIE-related_to-O:CURIE'


def test_camelcase_to_sentencecase():
    s = camelcase_to_sentencecase('NamedThing')
    assert s == 'named thing'


def test_snakecase_to_sentencecase():
    s = snakecase_to_sentencecase('named_thing')
    assert s == 'named thing'


def test_sentencecase_to_snakecase():
    s = sentencecase_to_snakecase('named thing')
    assert s == 'named_thing'


def test_sentencecase_to_camelcase():
    s = sentencecase_to_camelcase('named thing')
    assert s == 'NamedThing'


@pytest.mark.parametrize("query", [
    ('HGNC:11603', 'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=11603', 'https://identifiers.org/hgnc:11603')
])
def test_contract(query):
    curie = contract(query[1], prefix_maps=None, fallback=True)
    # get the CURIE
    assert curie == query[0]

    # provide custom prefix_maps, with fallback
    curie = contract(query[2], prefix_maps=[{'HGNC': 'https://identifiers.org/hgnc:'}], fallback=True)
    # get the CURIE
    assert curie == query[0]

    # provide custom prefix_maps, but no fallback
    curie = contract(query[2], prefix_maps=[{'HGNC': 'https://identifiers.org/hgnc:'}], fallback=False)
    # get the CURIE
    assert curie == query[0]

    # provide no prefix_maps, and no fallback
    curie = contract(query[2], prefix_maps=None, fallback=False)
    # get back the IRI
    assert curie == query[2]


@pytest.mark.parametrize("query", [
    ('HGNC:11603', 'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=11603', 'https://identifiers.org/hgnc:11603')
])
def test_expand(query):
    iri = expand(query[0], prefix_maps=None, fallback=True)
    # get the IRI
    assert iri == query[1]

    # provide custom prefix_maps, with fallback
    iri = expand(query[0], prefix_maps=[{'HGNC': 'https://identifiers.org/hgnc:'}], fallback=True)
    # get the alternate IRI
    assert iri == query[2]

    # provide custom prefix_maps, but no fallback
    iri = expand(query[0], prefix_maps=[{'hgnc': 'https://example.org/hgnc:'}], fallback=False)
    # get back the CURIE
    assert iri == query[0]

    # provide no prefix_maps, and no fallback
    iri = expand(query[0], prefix_maps=None, fallback=False)
    # get the IRI
    assert iri == query[1]


def test_generate_uuid():
    s = generate_uuid()
    assert s.startswith('urn:uuid:')
