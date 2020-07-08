import pytest

from kgx import PrefixManager


@pytest.mark.parametrize('query', [
    ('https://example.org/123', True),
    ('http://example.org/ABC', True),
    ('http://purl.obolibrary.org/obo/GO_0008150', True),
    ('GO:0008150', False)
])
def test_is_iri(query):
    assert PrefixManager.is_iri(query[0]) == query[1]


@pytest.mark.parametrize('query', [
    ('GO:0008150', True),
    ('CHEMBL.COMPOUND:12345', True),
    ('HP:0000000', True),
    ('GO_0008150', False),
    ('12345', False),
    (':12345', True)
])
def test_is_curie(query):
    assert PrefixManager.is_curie(query[0]) == query[1]


@pytest.mark.parametrize('query', [
    ('GO:0008150', 'GO'),
    ('CHEMBL.COMPOUND:12345', 'CHEMBL.COMPOUND'),
    ('HP:0000000', 'HP'),
    ('GO_0008150', None),
    ('12345', None),
    (':12345', '')
])
def test_get_prefix(query):
    assert PrefixManager.get_prefix(query[0]) == query[1]


@pytest.mark.parametrize('query', [
    ('GO:0008150', '0008150'),
    ('CHEMBL.COMPOUND:12345', '12345'),
    ('HP:0000000', '0000000'),
    ('GO_0008150', None),
    ('12345', None),
    (':12345', '12345')
])
def test_get_reference(query):
    assert PrefixManager.get_reference(query[0]) == query[1]


def test_prefix_manager():
    pm = PrefixManager()
    assert pm.prefix_map
    assert pm.reverse_prefix_map
    assert 'biolink' in pm.prefix_map
    assert ':' in pm.prefix_map


@pytest.mark.parametrize('query', [
    ('GO:0008150', 'http://purl.obolibrary.org/obo/GO_0008150'),
    ('HP:0000000', 'http://purl.obolibrary.org/obo/HP_0000000'),
    ('biolink:category', 'https://w3id.org/biolink/vocab/category'),
    ('biolink:related_to', 'https://w3id.org/biolink/vocab/related_to'),
    ('biolink:NamedThing', 'https://w3id.org/biolink/vocab/NamedThing'),
    ('HGNC:1103', 'http://identifiers.org/hgnc/1103')
])
def test_prefix_manager_expand(query):
    pm = PrefixManager()
    assert pm.expand(query[0]) == query[1]



@pytest.mark.parametrize('query', [
    ('http://purl.obolibrary.org/obo/GO_0008150', 'GO:0008150'),
    ('http://purl.obolibrary.org/obo/HP_0000000', 'HP:0000000'),
    ('https://w3id.org/biolink/vocab/category', 'biolink:category'),
    ('https://w3id.org/biolink/vocab/related_to', 'biolink:related_to'),
    ('https://w3id.org/biolink/vocab/NamedThing', 'biolink:NamedThing'),
    ('http://identifiers.org/hgnc/1103', 'HGNC:1103')
])
def test_prefix_manager_contract(query):
    pm = PrefixManager()
    assert pm.contract(query[0]) == query[1]

