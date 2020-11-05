import os

import pytest
from networkx import Graph, MultiDiGraph

from kgx import Transformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')

def get_graphs():
    g1 = MultiDiGraph()
    g1.name = 'Graph 1'
    g1.add_node('HGNC:12345', id='HGNC:12345', name='Test Gene', category=['biolink:Gene'])
    g1.add_node('B', id='B', name='Node B', category=['biolink:NamedThing'])
    g1.add_node('C', id='C', name='Node C', category=['biolink:NamedThing'])
    g1.add_edge('C', 'B', key='C-biolink:subclass_of-B', predicate='biolink:sub_class_of', relation='rdfs:subClassOf')
    g1.add_edge('B', 'A', key='B-biolink:subclass_of-A', predicate='biolink:sub_class_of', relation='rdfs:subClassOf', provided_by='Graph 1')
    return [g1]


def test_transformer():
    t = Transformer()
    assert isinstance(t.graph, Graph)
    assert t.is_empty()

    t.set_node_filter('category', {'biolink:Gene'})
    t.set_node_filter('category', {'biolink:Disease'})
    t.set_edge_filter('predicate', {'biolink:related_to'})
    t.set_edge_filter('predicate', {'biolink:interacts_with'})
    t.set_edge_filter('subject_category', {'biolink:Drug'})
    assert len(t.node_filters.keys()) == 1
    assert len(t.edge_filters.keys()) == 3
    assert 'category' in t.node_filters and len(t.node_filters['category']) == 3
    assert 'predicate' in t.edge_filters and len(t.edge_filters['predicate']) == 2
    assert 'subject_category' in t.edge_filters \
           and len(t.edge_filters['subject_category']) == 3 \
           and 'biolink:Gene' in t.edge_filters['subject_category']
    assert 'object_category' in t.edge_filters \
           and len(t.edge_filters['object_category']) == 3 \
           and 'biolink:Gene' in t.edge_filters['object_category']
    assert 'biolink:Drug' in t.node_filters['category']


def test_serialization():
    graphs = get_graphs()
    t1 = Transformer(source_graph=graphs[0])
    assert t1.is_empty() is False
    Transformer.dump_to_file(t1.graph, os.path.join(target_dir, 'graph_serialization.json'))

    new_graph = Transformer.restore_from_file(os.path.join(target_dir, 'graph_serialization.json'))
    t2 = Transformer(source_graph=new_graph)
    assert t1.is_empty() is False
    assert t2.graph.number_of_nodes() == t1.graph.number_of_nodes()
    assert t2.graph.number_of_edges() == t1.graph.number_of_edges()


@pytest.mark.parametrize('node', [
    {'name': 'Node A', 'description': 'Node without an ID'},
    {'node_id': 'A', 'description': 'Node without an ID and name'},
    {'name': 'Node A', 'description': 'Node A', 'category': 'biolink:NamedThing'}
])
def test_validate_incorrect_node(node):
    with pytest.raises(KeyError):
        Transformer.validate_node(node)


@pytest.mark.parametrize('node', [
    {'id': 'A', 'name': 'Node A', 'description': 'Node A', 'category': ['biolink:NamedThing']},
    {'id': 'A', 'name': 'Node A', 'description': 'Node A'}
])
def test_validate_correct_node(node):
    n = Transformer.validate_node(node)
    assert n is not None
    assert 'category' in n
    assert n['category'][0] == Transformer.DEFAULT_NODE_CATEGORY


@pytest.mark.parametrize('edge', [
    {'predicate': 'biolink:related_to'},
    {'subject': 'A', 'predicate': 'biolink:related_to'},
    {'subject': 'A', 'object': 'B'},
])
def test_validate_incorrect_edge(edge):
    with pytest.raises(KeyError):
        Transformer.validate_edge(edge)


@pytest.mark.parametrize('edge', [
    {'subject': 'A', 'object': 'B', 'predicate': 'biolink:related_to'},
    {'subject': 'A', 'object': 'B', 'predicate': 'biolink:related_to', 'relation': 'RO:000000'},
])
def test_validate_correct_edge(edge):
    e = Transformer.validate_edge(edge)
    assert e is not None

