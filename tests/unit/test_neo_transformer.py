import pytest
from neo4jrestclient.client import Node, Relationship
from networkx import MultiDiGraph

from kgx import NeoTransformer
from tests import clean_slate, check_container, CONTAINER_NAME, DEFAULT_NEO4J_URL, \
    DEFAULT_NEO4J_USERNAME, DEFAULT_NEO4J_PASSWORD


def get_graph(source):
    g1 = MultiDiGraph()
    g1.name = 'Graph 1'
    g1.add_node('A', id='A', name='Node A', category=['biolink:NamedThing'], source=source)
    g1.add_node('B', id='B', name='Node B', category=['biolink:NamedThing'], source=source)
    g1.add_node('C', id='C', name='Node C', category=['biolink:NamedThing'], source=source)
    g1.add_edge('B', 'A', subject='B', object='A', predicate='biolink:sub_class_of', source=source)

    g2 = MultiDiGraph()
    g2.add_node('A', id='A', source=source)
    g2.add_node('B', id='B', source=source)
    g2.add_node('C', id='C', source=source)
    g2.add_node('D', id='D', source=source)
    g2.add_node('E', id='E', source=source)
    g2.add_node('F', id='F', source=source)
    g2.add_edge('B', 'A', subject='B', object='A', predicate='biolink:sub_class_of', source=source)
    g2.add_edge('C', 'B', subject='C', object='B',  predicate='biolink:sub_class_of', source=source)
    g2.add_edge('D', 'C', subject='D', object='C',  predicate='biolink:sub_class_of', source=source)
    g2.add_edge('D', 'A', subject='D', object='A',  predicate='biolink:related_to', source=source)
    g2.add_edge('E', 'D', subject='E', object='D',  predicate='biolink:sub_class_of', source=source)
    g2.add_edge('F', 'D', subject='F', object='D',  predicate='biolink:sub_class_of', source=source)

    g3 = MultiDiGraph()
    g3.add_node('A', id='A', category=['biolink:NamedThing'], source=source)
    g3.add_node('B', id='B', category=['biolink:NamedThing'], source=source)
    g3.add_edge('A', 'B', subject='A', object='B', predicate='biolink:related_to', source=source)

    g4 = MultiDiGraph()
    g4.add_node('A', id='A', category=['biolink:Gene'], provided_by=source, source=source)
    g4.add_node('B', id='B', category=['biolink:Gene'], provided_by=source, source=source)
    g4.add_node('A1', id='A1', category=['biolink:Protein'], provided_by=source, source=source)
    g4.add_node('A2', id='A2', category=['biolink:Protein'], provided_by=source, source=source)
    g4.add_node('B1', id='B1', category=['biolink:Protein'], provided_by=source, source=source)
    g4.add_node('X', id='X', category=['biolink:Drug'], provided_by=source, source=source)
    g4.add_node('Y', id='Y', category=['biolink:Drug'], provided_by=source, source=source)
    g4.add_edge('A', 'A1', subject='A', object='A1', predicate='biolink:has_gene_product', provided_by=source, source=source)
    g4.add_edge('A', 'A2', subject='A', object='A2', predicate='biolink:has_gene_product', provided_by=source, source=source)
    g4.add_edge('B', 'B1', subject='B', object='B1', predicate='biolink:has_gene_product', provided_by=source, source=source)
    g4.add_edge('X', 'A1', subject='X', object='A1', predicate='biolink:interacts_with', provided_by=source, source=source)
    g4.add_edge('Y', 'B', subject='Y', object='B', predicate='biolink:interacts_with', provided_by=source, source=source)
    return [g1, g2, g3, g4]


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


@pytest.mark.skipif(not check_container(), reason=f'Container {CONTAINER_NAME} is not running')
@pytest.mark.parametrize('query', [
    (get_graph('kgx-unit-test')[0], 3, 1),
    (get_graph('kgx-unit-test')[1], 6, 6)
])
def test_load(clean_slate, query):

    t = NeoTransformer(None, uri=DEFAULT_NEO4J_URL, username=DEFAULT_NEO4J_USERNAME, password=DEFAULT_NEO4J_PASSWORD)
    print(f"BEFORE: {t.neo4j_report()}")
    t.graph = query[0]
    t.save()
    print(f"AFTER: {t.neo4j_report()}")

    nr = t.http_driver.query("MATCH (n) RETURN count(n)")
    [node_counts] = [x for x in nr][0]
    assert node_counts == query[1]

    er = t.http_driver.query("MATCH ()-[p]->() RETURN count(p)")
    [edge_counts] = [x for x in er][0]
    assert edge_counts == query[2]


@pytest.mark.skipif(not check_container(), reason=f'Container {CONTAINER_NAME} is not running')
def test_save_merge(clean_slate):
    g = get_graph('kgx-unit-test')[2]
    t = NeoTransformer(g, uri=DEFAULT_NEO4J_URL, username=DEFAULT_NEO4J_USERNAME, password=DEFAULT_NEO4J_PASSWORD)
    t.save()

    t.graph.add_node('B', id='B', publications=['PMID:1', 'PMID:2'], category=['biolink:NamedThing'])
    t.graph.add_node('C', id='C', source='kgx-unit-test')
    t.graph.add_edge('A', 'B', subject='A', object='B', predicate='biolink:related_to', test_prop='VAL123')
    assert t.graph.number_of_nodes() == 3
    t.save()

    nr = t.http_driver.query("MATCH (n) RETURN n")
    for node in nr:
        data = node[0]['data']
        if data['id'] == 'B':
            assert 'category' in data and data['category'] == ['biolink:NamedThing']
            assert 'publications' in data and data['publications'] == ['PMID:1', 'PMID:2']

    er = t.http_driver.query("MATCH ()-[p]-() RETURN p", data_contents=True, returns=(Node, Relationship, Node))
    for edge in er:
        data = edge[0].properties
        # assert data['id'] == 'A-biolink:related_to-B'
        assert data['subject'] == 'A'
        assert data['object'] == 'B'
        assert data['predicate'] == 'biolink:related_to'
        assert data['test_prop'] == 'VAL123'


@pytest.mark.skipif(not check_container(), reason=f'Container {CONTAINER_NAME} is not running')
@pytest.mark.parametrize('query', [
    (get_graph('kgx-unit-test')[3], {'category': {'biolink:Gene'}}, 2, ['A', 'B']),
    (get_graph('kgx-unit-test')[3], {'category': {'biolink:Gene', 'biolink:Protein'}}, 5, ['A', 'B', 'A1', 'A2', 'B1']),
    (get_graph('kgx-unit-test')[3], {'provided_by': {'kgx-unit-test'}}, 7, ['A', 'B', 'A1', 'A2', 'B1', 'X', 'Y']),
])
def test_get_nodes(clean_slate, query):
    g = query[0]
    t = NeoTransformer(g, uri=DEFAULT_NEO4J_URL, username=DEFAULT_NEO4J_USERNAME, password=DEFAULT_NEO4J_PASSWORD)
    t.save()

    for k, v in query[1].items():
        t.set_node_filter(k, v)
    nodes = t.get_nodes()

    assert len(nodes) == query[2]
    node_ids = [x['id'] for x in nodes]
    for x in query[3]:
        assert x in node_ids


@pytest.mark.skipif(not check_container(), reason=f'Container {CONTAINER_NAME} is not running')
@pytest.mark.parametrize('query', [
    (get_graph('kgx-unit-test')[3], {'subject_category': {'biolink:Gene'}, 'object_category': {'biolink:Protein'}}, 3),
    (get_graph('kgx-unit-test')[3], {'subject_category': {'biolink:Drug'}, 'object_category': {'biolink:Gene'}}, 1),
    (get_graph('kgx-unit-test')[3], {'provided_by': {'kgx-unit-test'}}, 5),
    (get_graph('kgx-unit-test')[3], {'predicate': {'biolink:interacts_with'}}, 2),
    (get_graph('kgx-unit-test')[3], {'subject_category': {'biolink:Drug'}, 'predicate': {'biolink:interacts_with'}}, 2),
    (get_graph('kgx-unit-test')[3], {'subject_category': {'biolink:Gene', 'biolink:Protein'}, 'predicate': {'biolink:interacts_with'}}, 0),
])
def test_get_edges(clean_slate, query):
    g = get_graph('kgx-unit-test')[3]
    t = NeoTransformer(g, uri=DEFAULT_NEO4J_URL, username=DEFAULT_NEO4J_USERNAME, password=DEFAULT_NEO4J_PASSWORD)
    t.save()

    for k, v in query[1].items():
        t.set_edge_filter(k, v)

    edges = t.get_edges()
    edge_list = [x[1] for x in edges]
    assert len(edges) == query[2]
