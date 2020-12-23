import os
import pytest

from kgx import Transformer
from kgx.graph.base_graph import BaseGraph
from kgx.graph.nx_graph import NxGraph

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')


def get_graphs():
    g1 = NxGraph()
    g1.name = 'Graph 1'
    g1.add_node('HGNC:12345', **{'biolink:id': 'HGNC:12345', 'biolink:name': 'Test Gene', 'biolink:category': ['biolink:Gene']})
    g1.add_node('B', **{'biolink:id': 'B', 'biolink:name': 'Node B', 'biolink:category': ['biolink:NamedThing']})
    g1.add_node('C', **{'biolink:id': 'C', 'biolink:name': 'Node C', 'biolink:category': ['biolink:NamedThing']})
    g1.add_edge('C', 'B', **{'edge_key': 'C-biolink:subclass_of-B', 'biolink:predicate': 'biolink:subclass_of', 'biolink:relation': 'rdfs:subClassOf'})
    g1.add_edge('B', 'A', **{'edge_key': 'B-biolink:subclass_of-A', 'biolink:predicate': 'biolink:subclass_of', 'biolink:relation': 'rdfs:subClassOf', 'biolink:provided_by': 'Graph 1'})
    return [g1]


def test_transformer():
    t = Transformer()
    assert isinstance(t.graph, BaseGraph)
    assert t.is_empty()

    t.set_node_filter('category', {'biolink:Gene'})
    t.set_node_filter('category', {'biolink:Disease'})
    t.set_edge_filter('predicate', {'biolink:related_to'})
    t.set_edge_filter('predicate', {'biolink:interacts_with'})
    t.set_edge_filter('subject_category', {'biolink:Drug'})
    assert len(t.node_filters.keys()) == 1
    assert len(t.edge_filters.keys()) == 3
    assert 'biolink:category' in t.node_filters and len(t.node_filters['biolink:category']) == 3
    assert 'biolink:predicate' in t.edge_filters and len(t.edge_filters['biolink:predicate']) == 2
    assert 'subject_category' in t.edge_filters \
           and len(t.edge_filters['subject_category']) == 3 \
           and 'biolink:Gene' in t.edge_filters['subject_category']
    assert 'object_category' in t.edge_filters \
           and len(t.edge_filters['object_category']) == 3 \
           and 'biolink:Gene' in t.edge_filters['object_category']
    assert 'biolink:Drug' in t.node_filters['biolink:category']


@pytest.mark.parametrize('node', [
    {'biolink:name': 'Node A', 'biolink:description': 'Node without an ID'},
    {'node_id': 'A', 'biolink:description': 'Node without an ID and name'},
    {'biolink:name': 'Node A', 'biolink:description': 'Node A', 'biolink:category': 'biolink:NamedThing'}
])
def test_validate_incorrect_node(node):
    with pytest.raises(KeyError):
        Transformer.validate_node(node)


@pytest.mark.parametrize('node', [
    {'biolink:id': 'A', 'biolink:name': 'Node A', 'biolink:description': 'Node A', 'biolink:category': ['biolink:NamedThing']},
    {'biolink:id': 'A', 'biolink:name': 'Node A', 'biolink:description': 'Node A'}
])
def test_validate_correct_node(node):
    n = Transformer.validate_node(node)
    assert n is not None
    assert 'biolink:category' in n
    assert n['biolink:category'][0] == Transformer.DEFAULT_NODE_CATEGORY


@pytest.mark.parametrize('edge', [
    {'biolink:predicate': 'biolink:related_to'},
    {'biolink:subject': 'A', 'biolink:predicate': 'biolink:related_to'},
    {'biolink:subject': 'A', 'biolink:object': 'B'},
])
def test_validate_incorrect_edge(edge):
    with pytest.raises(KeyError):
        Transformer.validate_edge(edge)


@pytest.mark.parametrize('edge', [
    {'biolink:subject': 'A', 'biolink:object': 'B', 'biolink:predicate': 'biolink:related_to'},
    {'biolink:subject': 'A', 'biolink:object': 'B', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'RO:000000'},
])
def test_validate_correct_edge(edge):
    e = Transformer.validate_edge(edge)
    assert e is not None

