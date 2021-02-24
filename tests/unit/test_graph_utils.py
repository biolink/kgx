import pytest

from kgx.graph.nx_graph import NxGraph
from kgx.utils.graph_utils import get_parents, get_ancestors, curie_lookup
from kgx.graph_operations import fold_predicate, unfold_node_property, remove_singleton_nodes


def get_graphs():
    g1 = NxGraph()
    g1.add_edge('B', 'A', **{'predicate': 'biolink:sub_class_of'})
    g1.add_edge('C', 'B', **{'predicate': 'biolink:sub_class_of'})
    g1.add_edge('D', 'C', **{'predicate': 'biolink:sub_class_of'})
    g1.add_edge('D', 'A', **{'predicate': 'biolink:related_to'})
    g1.add_edge('E', 'D', **{'predicate': 'biolink:sub_class_of'})
    g1.add_edge('F', 'D', **{'predicate': 'biolink:sub_class_of'})

    g2 = NxGraph()
    g2.name = 'Graph 1'
    g2.add_node('HGNC:12345', id='HGNC:12345', name='Test Gene', category=['biolink:NamedThing'], alias='NCBIGene:54321', same_as='UniProtKB:54321')
    g2.add_node('B', id='B', name='Node B', category=['biolink:NamedThing'], alias='Z')
    g2.add_node('C', id='C', name='Node C', category=['biolink:NamedThing'])
    g2.add_edge('C', 'B', edge_key='C-biolink:subclass_of-B', subject='C', object='B', predicate='biolink:subclass_of', relation='rdfs:subClassOf', provided_by='Graph 1', publications=[1], pubs=['PMID:123456'])
    g2.add_edge('B', 'A', edge_key='B-biolink:subclass_of-A', subject='B', object='A', predicate='biolink:subclass_of', relation='rdfs:subClassOf', provided_by='Graph 1')
    g2.add_edge('C', 'c', edge_key='C-biolink:exact_match-B', subject='C', object='c', predicate='biolink:exact_match', relation='skos:exactMatch', provided_by='Graph 1')

    return [g1, g2]



def test_get_parents():
    query = ('E', ['D'])
    graph = get_graphs()[0]
    parents = get_parents(graph, query[0], relations=['biolink:sub_class_of'])
    assert len(parents) == len(query[1])
    assert parents == query[1]


def test_get_ancestors():
    query = ('E', ['D', 'C', 'B', 'A'])
    graph = get_graphs()[0]
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


def test_fold_predicate1():
    g = get_graphs()[1]
    fold_predicate(g, 'biolink:exact_match')
    assert not g.has_edge('C', 'c')
    n = g.nodes(data=True)['C']
    assert 'biolink:exact_match' in n and n['biolink:exact_match'] == 'c'


def test_fold_predicate2():
    g = get_graphs()[1]
    fold_predicate(g, 'biolink:exact_match', remove_prefix=True)
    assert not g.has_edge('C', 'c')
    n = g.nodes(data=True)['C']
    assert 'exact_match' in n and n['exact_match'] == 'c'


def test_unfold_node_property1():
    g = get_graphs()[1]
    unfold_node_property(g, 'same_as')
    assert 'same_as' not in g.nodes()['HGNC:12345']
    assert g.has_edge('HGNC:12345', 'UniProtKB:54321')
    e = list(dict(g.get_edge('HGNC:12345', 'UniProtKB:54321')).values())[0]
    assert 'subject' in e and e['subject'] == 'HGNC:12345'
    assert 'predicate' in e and e['predicate'] == 'same_as'
    assert 'object' in e and e['object'] == 'UniProtKB:54321'


def test_unfold_node_property2():
    g = get_graphs()[1]
    unfold_node_property(g, 'same_as', prefix='biolink')
    assert 'same_as' not in g.nodes()['HGNC:12345']
    assert g.has_edge('HGNC:12345', 'UniProtKB:54321')
    e = list(dict(g.get_edge('HGNC:12345', 'UniProtKB:54321')).values())[0]
    assert 'subject' in e and e['subject'] == 'HGNC:12345'
    assert 'predicate' in e and e['predicate'] == 'biolink:same_as'
    assert 'object' in e and e['object'] == 'UniProtKB:54321'


def test_remove_singleton_nodes():
    g = NxGraph()
    g.add_edge('A', 'B')
    g.add_edge('B', 'C')
    g.add_edge('C', 'D')
    g.add_edge('B', 'D')
    g.add_node('X')
    g.add_node('Y')
    assert g.number_of_nodes() == 6
    assert g.number_of_edges() == 4
    remove_singleton_nodes(g)
    assert g.number_of_nodes() == 4
    assert g.number_of_edges() == 4
