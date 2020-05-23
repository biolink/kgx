import pytest
from networkx import MultiDiGraph

from kgx import NeoTransformer


def get_graph(provided_by):
    graph = MultiDiGraph()
    graph.add_edge('B', 'A', subject='B', object='A', edge_label='biolink:sub_class_of', provided_by=provided_by)
    graph.add_edge('C', 'B', subject='C', object='B',  edge_label='biolink:sub_class_of', provided_by=provided_by)
    graph.add_edge('D', 'C', subject='D', object='C',  edge_label='biolink:sub_class_of', provided_by=provided_by)
    graph.add_edge('D', 'A', subject='D', object='A',  edge_label='biolink:related_to', provided_by=provided_by)
    graph.add_edge('E', 'D', subject='E', object='D',  edge_label='biolink:sub_class_of', provided_by=provided_by)
    graph.add_edge('F', 'D', subject='F', object='D',  edge_label='biolink:sub_class_of', provided_by=provided_by)
    return graph


def test_sanitize_category():
    categories = ['biolink:Gene', 'biolink:GeneOrGeneProduct']
    s = NeoTransformer.sanitize_category(categories)
    assert s == ['`biolink:Gene`', '`biolink:GeneOrGeneProduct`']


@pytest.mark.parametrize('category', [
    'biolink:Gene',
    'biolink:GeneOrGeneProduct',
    'biolink:NamedThing'
])
def test_create_constraint_query(category):
    sanitized_category = NeoTransformer.sanitize_category([category])
    q = NeoTransformer.create_constraint_query(sanitized_category)
    assert q == f"CREATE CONSTRAINT ON (n:{sanitized_category}) ASSERT n.id IS UNIQUE"
