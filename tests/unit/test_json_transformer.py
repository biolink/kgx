import os

from kgx import JsonTransformer
from tests import RESOURCE_DIR, TARGET_DIR


def test_json_load():
    t = JsonTransformer()
    t.parse(os.path.join(RESOURCE_DIR, 'valid.json'))
    assert t.graph.number_of_nodes() == 6
    assert t.graph.number_of_edges() == 5

    n = t.graph.nodes()['MONDO:0017148']
    assert isinstance(n, dict)
    assert 'id' in n and n['id'] == 'MONDO:0017148'
    assert n['name'] == 'heritable pulmonary arterial hypertension'
    assert n['category'][0] == 'biolink:Disease'

    data = t.graph.get_edge('HGNC:11603', 'MONDO:0017148')
    assert len(data.keys()) == 1
    data = data.popitem()[1]
    assert data['subject'] == 'HGNC:11603'
    assert data['object'] == 'MONDO:0017148'
    assert data['predicate'] == 'biolink:related_to'
    assert data['relation'] == 'RO:0004013'


def test_json_save():
    t = JsonTransformer()
    t.parse(os.path.join(RESOURCE_DIR, 'valid.json'))
    assert t.graph.number_of_nodes() == 6
    assert t.graph.number_of_edges() == 5

    t.save(os.path.join(TARGET_DIR, 'graph.json'))
    assert os.path.exists(os.path.join(TARGET_DIR, 'graph.json'))


def test_filters():
    t = JsonTransformer()
    t.set_node_filter('category', {'biolink:Disease'})
    t.set_edge_filter('category', {'biolink:causes'})
    t.parse(os.path.join(RESOURCE_DIR, 'valid.json'))
    assert t.graph.number_of_nodes() == 5
    assert t.graph.number_of_edges() == 0

