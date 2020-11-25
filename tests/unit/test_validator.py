import pytest

from kgx import Validator


@pytest.mark.parametrize('prefix', [
    'GO',
    'HP',
    'MONDO',
    'HGNC',
    'UniProtKB'
])
def test_get_all_prefixes(prefix):
    prefixes = Validator.get_all_prefixes()
    assert prefix in prefixes


@pytest.mark.parametrize('property', [
    'id',
    'name',
    'category'
])
def test_get_required_node_properties(property):
    properties = Validator.get_required_node_properties()
    assert property in properties


@pytest.mark.parametrize('property', [
    'subject',
    'object',
    'edge_label',
    'relation'
])
def test_get_required_edge_properties(property):
    properties = Validator.get_required_edge_properties()
    assert property in properties


@pytest.mark.parametrize('query', [
    ('A:123', {}, False),
    ('A:123', {'id': 'A:123'}, False),
    ('A:123', {'id': 'A:123', 'name': 'Node A:123'}, False),
    ('A:123', {'id': 'A:123', 'name': 'Node A:123', 'category': ['biolink:NamedThing']}, True),
])
def test_validate_node_properties(query):
    required_properties = Validator.get_required_node_properties()
    e = Validator.validate_node_properties(query[0], query[1], required_properties)
    assert (len(e) == 0) == query[2]


@pytest.mark.parametrize('query', [
    ('A:123', 'X:1', {}, False),
    ('A:123', 'X:1', {'edge_label': 'biolink:related_to'}, False),
    ('A:123', 'X:1', {'subject': 'A:123', 'edge_label': 'biolink:related_to'}, False),
    ('A:123', 'X:1', {'subject': 'A:123', 'object': 'X:1', 'edge_label': 'biolink:related_to'}, False),
    ('A:123', 'X:1', {'id': 'A:123-biolink:related_to-X:1', 'subject': 'A:123', 'object': 'X:1', 'edge_label': 'biolink:related_to', 'relation': 'biolink:related_to'}, True),
    ('A:123', 'X:1', {'association_id': 'Edge A-X', 'subject': 'A:123', 'object': 'X:1', 'edge_label': 'biolink:related_to', 'relation': 'biolink:related_to'}, True),
])
def test_validate_edge_properties(query):
    required_properties = Validator.get_required_edge_properties()
    e = Validator.validate_edge_properties(query[0], query[1], query[2], required_properties)
    print(Validator.report(e))
    assert (len(e) == 0) == query[3]


@pytest.mark.parametrize('query', [
    ('A:123', {'id': 'A:123', 'name': 'Node A:123', 'category': ['biolink:NamedThing']}, True),
    ('A:123', {'id': 'A:123', 'name': 'Node A:123', 'category': 'biolink:NamedThing'}, False),
    ('A:123', {'id': 'A:123', 'name': ['Node A:123'], 'category': 'biolink:NamedThing'}, False),
    ('A:123', {'id': 'A:123', 'name': 'Node A:123', 'category': ['biolink:NamedThing'], 'publications': 'PMID:789'}, False),
    ('A:123', {'id': 'A:123', 'name': 'Node A:123', 'category': ['biolink:NamedThing'], 'publications': ['PMID:789']}, True),
])
def test_validate_node_property_types(query):
    e = Validator.validate_node_property_types(query[0], query[1])
    assert (len(e) == 0) == query[2]


@pytest.mark.parametrize('query', [
    ('A:123', 'X:1', {'id': 'A:123-biolink:related_to-X:1', 'subject': 'A:123', 'object': 'X:1', 'edge_label': 'biolink:related_to', 'relation': 'biolink:related_to'}, True),
    ('A:123', 'X:1', {'id': 'A:123-biolink:related_to-X:1', 'subject': 'A:123', 'object': 'X:1', 'edge_label': ['biolink:related_to'], 'relation': 'biolink:related_to'}, False),
    ('A:123', 'X:1', {'id': 'A:123-biolink:related_to-X:1', 'subject': 'A:123', 'object': 'X:1', 'edge_label': 'biolink:related_to', 'relation': ['biolink:related_to']}, False),
])
def test_validate_edge_property_types(query):
    e = Validator.validate_edge_property_types(query[0], query[1], query[2])
    assert (len(e) == 0) == query[3]


@pytest.mark.parametrize('query', [
    ('HGNC:123', {'id': 'HGNC:123', 'name': 'Node HGNC:123', 'category': ['biolink:NamedThing']}, True),
    ('HGNC_123', {'id': 'HGNC_123', 'name': 'Node HGNC_123', 'category': ['biolink:NamedThing']}, False),
    ('A:123', {'id': 'A:123', 'name': 'Node A:123', 'category': ['biolink:NamedThing']}, False)
])
def test_validate_node_property_values(query):
    e = Validator.validate_node_property_values(query[0], query[1])
    assert (len(e) == 0) == query[2]


@pytest.mark.parametrize('query', [
    ('A:123', 'X:1', {'id': 'A:123-biolink:related_to-X:1', 'subject': 'A:123', 'object': 'X:1', 'edge_label': 'biolink:related_to', 'relation': 'biolink:related_to'}, False),
    ('HGNC:123', 'X:1', {'id': 'HGNC:123-biolink:related_to-X:1', 'subject': 'A:123', 'object': 'X:1', 'edge_label': 'biolink:related_to', 'relation': 'biolink:related_to'}, False),
    ('HGNC:123', 'MONDO:1', {'id': 'HGNC:123-biolink:related_to-MONDO:1', 'subject': 'A:123', 'object': 'X:1', 'edge_label': 'biolink:related_to', 'relation': 'biolink:related_to'}, True),
    ('HGNC_123', 'MONDO:1', {'id': 'HGNC_123-biolink:related_to-MONDO:1', 'subject': 'A:123', 'object': 'X:1', 'edge_label': 'biolink:related_to', 'relation': 'biolink:related_to'}, False)
])
def test_validate_edge_property_values(query):
    e = Validator.validate_edge_property_values(query[0], query[1], query[2])
    assert (len(e) == 0) == query[3]


@pytest.mark.parametrize('query', [
    ('HGNC:123', {'id': 'HGNC:123', 'name': 'Node HGNC:123', 'category': ['biolink:NamedThing']}, True),
    ('A:123', {'id': 'A:123', 'name': 'Node A:123', 'category': ['biolink:NamedThing', 'biolink:Gene']}, True),
    ('A:123', {'id': 'A:123', 'name': 'Node A:123', 'category': ['NamedThing']}, True),
    ('A:123', {'id': 'A:123', 'name': 'Node A:123', 'category': ['Gene']}, True),
    ('A:123', {'id': 'A:123', 'name': 'Node A:123', 'category': ['GENE']}, False)
])
def test_validate_categories(query):
    e = Validator.validate_categories(query[0], query[1])
    assert (len(e) == 0) == query[2]


@pytest.mark.parametrize('query', [
    ('HGNC:123', 'MONDO:1', {'id': 'HGNC:123-biolink:related_to-MONDO:1', 'subject': 'A:123', 'object': 'X:1', 'edge_label': 'biolink:related_to', 'relation': 'biolink:related_to'}, True),
    ('HGNC:123', 'MONDO:1', {'id': 'HGNC:123-biolink:related_to-MONDO:1', 'subject': 'A:123', 'object': 'X:1', 'edge_label': 'related_to', 'relation': 'biolink:related_to'}, True),
    ('HGNC:123', 'MONDO:1', {'id': 'HGNC:123-biolink:related_to-MONDO:1', 'subject': 'A:123', 'object': 'X:1', 'edge_label': 'related to', 'relation': 'biolink:related_to'}, False),
    ('HGNC:123', 'MONDO:1', {'id': 'HGNC:123-biolink:related_to-MONDO:1', 'subject': 'A:123', 'object': 'X:1', 'edge_label': 'xyz', 'relation': 'biolink:related_to'}, False),
])
def test_validate_edge_label(query):
    e = Validator.validate_edge_label(query[0], query[1], query[2])
    assert (len(e) == 0) == query[3]