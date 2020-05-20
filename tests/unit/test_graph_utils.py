import pytest
from networkx import MultiDiGraph

from kgx.utils.graph_utils import get_parents, get_ancestors, curie_lookup


def get_graph():
    graph = MultiDiGraph()
    graph.add_edge('B', 'A', edge_label='biolink:sub_class_of')
    graph.add_edge('C', 'B', edge_label='biolink:sub_class_of')
    graph.add_edge('D', 'C', edge_label='biolink:sub_class_of')
    graph.add_edge('D', 'A', edge_label='biolink:related_to')
    graph.add_edge('E', 'D', edge_label='biolink:sub_class_of')
    graph.add_edge('F', 'D', edge_label='biolink:sub_class_of')
    return graph


def test_get_parents():
    query = ('E', ['D'])
    graph = get_graph()
    parents = get_parents(graph, query[0], relations=['biolink:sub_class_of'])
    assert len(parents) == len(query[1])
    assert parents == query[1]


def test_get_ancestors():
    query = ('E', ['D', 'C', 'B', 'A'])
    graph = get_graph()
    parents = get_ancestors(graph, query[0], relations=['biolink:sub_class_of'])
    assert len(parents) == len(query[1])
    assert parents == query[1]


@pytest.mark.skip(reason="To be implemented")
def test_get_category_via_superclass():
    pass


@pytest.mark.parametrize('query', [
    ('rdfs:subClassOf', 'sub_class_of'),
    ('owl:equivalentClass', 'equivalent_class'),
    ('RO:0000091', 'has_disposition')
])
def test_curie_lookup(query):
    s = curie_lookup(query[0])
    assert s == query[1]
