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
    'biolink:id',
    'biolink:category'
])
def test_get_required_node_properties(property):
    properties = Validator.get_required_node_properties()
    assert property in properties


@pytest.mark.parametrize('property', [
    'biolink:subject',
    'biolink:object',
    'biolink:predicate',
    'biolink:relation'
])
def test_get_required_edge_properties(property):
    properties = Validator.get_required_edge_properties()
    assert property in properties


@pytest.mark.parametrize('query', [
    ('A:123', {}, False),
    ('A:123', {'biolink:id': 'A:123'}, False),
    ('A:123', {'biolink:id': 'A:123', 'biolink:name': 'Node A:123'}, False),
    ('A:123', {'biolink:id': 'A:123', 'biolink:name': 'Node A:123', 'biolink:category': ['biolink:NamedThing']}, True),
])
def test_validate_node_properties(query):
    required_properties = Validator.get_required_node_properties()
    e = Validator.validate_node_properties(query[0], query[1], required_properties)
    assert (len(e) == 0) == query[2]


@pytest.mark.parametrize('query', [
    ('A:123', 'X:1', {}, False),
    ('A:123', 'X:1', {'biolink:predicate': 'biolink:related_to'}, False),
    ('A:123', 'X:1', {'biolink:subject': 'A:123', 'biolink:predicate': 'biolink:related_to'}, False),
    ('A:123', 'X:1', {'biolink:subject': 'A:123', 'biolink:object': 'X:1', 'biolink:predicate': 'biolink:related_to'}, False),
    ('A:123', 'X:1', {'biolink:id': 'A:123-biolink:related_to-X:1', 'biolink:subject': 'A:123', 'biolink:object': 'X:1', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to', 'biolink:category': ['biolink:Association']}, True),
    ('A:123', 'X:1', {'biolink:id': 'Edge A-X', 'biolink:subject': 'A:123', 'biolink:object': 'X:1', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to', 'biolink:category': ['biolink:Association']}, True),
])
def test_validate_edge_properties(query):
    required_properties = Validator.get_required_edge_properties()
    e = Validator.validate_edge_properties(query[0], query[1], query[2], required_properties)
    print(Validator.report(e))
    assert (len(e) == 0) == query[3]


@pytest.mark.parametrize('query', [
    ('A:123', {'biolink:id': 'A:123', 'biolink:name': 'Node A:123', 'biolink:category': ['biolink:NamedThing']}, True),
    ('A:123', {'biolink:id': 'A:123', 'biolink:name': 'Node A:123', 'biolink:category': 'biolink:NamedThing'}, False),
    ('A:123', {'biolink:id': 'A:123', 'biolink:name': ['Node A:123'], 'biolink:category': 'biolink:NamedThing'}, False),
    ('A:123', {'biolink:id': 'A:123', 'biolink:name': 'Node A:123', 'biolink:category': ['biolink:NamedThing'], 'biolink:publications': 'PMID:789'}, False),
    ('A:123', {'biolink:id': 'A:123', 'biolink:name': 'Node A:123', 'biolink:category': ['biolink:NamedThing'], 'biolink:publications': ['PMID:789']}, True),
])
def test_validate_node_property_types(query):
    e = Validator.validate_node_property_types(query[0], query[1])
    assert (len(e) == 0) == query[2]


@pytest.mark.parametrize('query', [
    ('A:123', 'X:1', {'biolink:id': 'A:123-biolink:related_to-X:1', 'biolink:subject': 'A:123', 'biolink:object': 'X:1', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to'}, True),
    ('A:123', 'X:1', {'biolink:id': 'A:123-biolink:related_to-X:1', 'biolink:subject': 'A:123', 'biolink:object': 'X:1', 'biolink:predicate': ['biolink:related_to'], 'biolink:relation': 'biolink:related_to'}, False),
    ('A:123', 'X:1', {'biolink:id': 'A:123-biolink:related_to-X:1', 'biolink:subject': 'A:123', 'biolink:object': 'X:1', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': ['biolink:related_to']}, False),
])
def test_validate_edge_property_types(query):
    e = Validator.validate_edge_property_types(query[0], query[1], query[2])
    assert (len(e) == 0) == query[3]


@pytest.mark.parametrize('query', [
    ('HGNC:123', {'biolink:id': 'HGNC:123', 'biolink:name': 'Node HGNC:123', 'biolink:category': ['biolink:NamedThing']}, True),
    ('HGNC_123', {'biolink:id': 'HGNC_123', 'biolink:name': 'Node HGNC_123', 'biolink:category': ['biolink:NamedThing']}, False),
    ('A:123', {'biolink:id': 'A:123', 'biolink:name': 'Node A:123', 'biolink:category': ['biolink:NamedThing']}, False)
])
def test_validate_node_property_values(query):
    e = Validator.validate_node_property_values(query[0], query[1])
    assert (len(e) == 0) == query[2]


@pytest.mark.parametrize('query', [
    ('A:123', 'X:1', {'biolink:id': 'A:123-biolink:related_to-X:1', 'biolink:subject': 'A:123', 'biolink:object': 'X:1', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to'}, False),
    ('HGNC:123', 'X:1', {'biolink:id': 'HGNC:123-biolink:related_to-X:1', 'biolink:subject': 'A:123', 'biolink:object': 'X:1', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to'}, False),
    ('HGNC:123', 'MONDO:1', {'biolink:id': 'HGNC:123-biolink:related_to-MONDO:1', 'biolink:subject': 'A:123', 'biolink:object': 'X:1', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to'}, True),
    ('HGNC_123', 'MONDO:1', {'biolink:id': 'HGNC_123-biolink:related_to-MONDO:1', 'biolink:subject': 'A:123', 'biolink:object': 'X:1', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to'}, False)
])
def test_validate_edge_property_values(query):
    e = Validator.validate_edge_property_values(query[0], query[1], query[2])
    assert (len(e) == 0) == query[3]


@pytest.mark.parametrize('query', [
    ('HGNC:123', {'biolink:id': 'HGNC:123', 'biolink:name': 'Node HGNC:123', 'biolink:category': ['biolink:NamedThing']}, True),
    ('A:123', {'biolink:id': 'A:123', 'biolink:name': 'Node A:123', 'biolink:category': ['biolink:NamedThing', 'biolink:Gene']}, True),
    ('A:123', {'biolink:id': 'A:123', 'biolink:name': 'Node A:123', 'biolink:category': ['NamedThing']}, True),
    ('A:123', {'biolink:id': 'A:123', 'biolink:name': 'Node A:123', 'biolink:category': ['Gene']}, True),
    ('A:123', {'biolink:id': 'A:123', 'biolink:name': 'Node A:123', 'biolink:category': ['GENE']}, False)
])
def test_validate_categories(query):
    e = Validator.validate_categories(query[0], query[1])
    assert (len(e) == 0) == query[2]


@pytest.mark.parametrize('query', [
    ('HGNC:123', 'MONDO:1', {'biolink:id': 'HGNC:123-biolink:related_to-MONDO:1', 'biolink:subject': 'A:123', 'biolink:object': 'X:1', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to'}, True),
    ('HGNC:123', 'MONDO:1', {'biolink:id': 'HGNC:123-biolink:related_to-MONDO:1', 'biolink:subject': 'A:123', 'biolink:object': 'X:1', 'biolink:predicate': 'related_to', 'biolink:relation': 'biolink:related_to'}, True),
    ('HGNC:123', 'MONDO:1', {'biolink:id': 'HGNC:123-biolink:related_to-MONDO:1', 'biolink:subject': 'A:123', 'biolink:object': 'X:1', 'biolink:predicate': 'related to', 'biolink:relation': 'biolink:related_to'}, False),
    ('HGNC:123', 'MONDO:1', {'biolink:id': 'HGNC:123-biolink:related_to-MONDO:1', 'biolink:subject': 'A:123', 'biolink:object': 'X:1', 'biolink:predicate': 'xyz', 'biolink:relation': 'biolink:related_to'}, False),
])
def test_validate_edge_label(query):
    e = Validator.validate_edge_predicate(query[0], query[1], query[2])
    assert (len(e) == 0) == query[3]