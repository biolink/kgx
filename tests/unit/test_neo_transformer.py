import pytest
from neo4jrestclient.client import Node, Relationship

from kgx import NeoTransformer
from kgx.graph.nx_graph import NxGraph
from tests import clean_slate, check_container, CONTAINER_NAME, DEFAULT_NEO4J_URL, \
    DEFAULT_NEO4J_USERNAME, DEFAULT_NEO4J_PASSWORD


def get_graph(source):
    g1 = NxGraph()
    g1.name = 'Graph 1'
    g1.add_node('A', **{'biolink:id': 'A', 'biolink:name': 'Node A', 'biolink:category': ['biolink:NamedThing'], 'biolink:source': source})
    g1.add_node('B', **{'biolink:id': 'B', 'biolink:name': 'Node B', 'biolink:category': ['biolink:NamedThing'], 'biolink:source': source})
    g1.add_node('C', **{'biolink:id': 'C', 'biolink:name': 'Node C', 'biolink:category': ['biolink:NamedThing'], 'biolink:source': source})
    g1.add_edge('B', 'A', **{'biolink:subject': 'B', 'biolink:object': 'A', 'biolink:predicate': 'biolink:subclass_of', 'biolink:source': source})

    g2 = NxGraph()
    g2.add_node('A', **{'biolink:id': 'A', 'source': source})
    g2.add_node('B', **{'biolink:id': 'B', 'source': source})
    g2.add_node('C', **{'biolink:id': 'C', 'source': source})
    g2.add_node('D', **{'biolink:id': 'D', 'source': source})
    g2.add_node('E', **{'biolink:id': 'E', 'source': source})
    g2.add_node('F', **{'biolink:id': 'F', 'source': source})
    g2.add_edge('B', 'A', **{'biolink:subject': 'B', 'biolink:object': 'A', 'biolink:predicate': 'biolink:subclass_of', 'biolink:source': source})
    g2.add_edge('C', 'B', **{'biolink:subject': 'C', 'biolink:object': 'B', 'biolink:predicate': 'biolink:subclass_of', 'biolink:source': source})
    g2.add_edge('D', 'C', **{'biolink:subject': 'D', 'biolink:object': 'C', 'biolink:predicate': 'biolink:subclass_of', 'biolink:source': source})
    g2.add_edge('D', 'A', **{'biolink:subject': 'D', 'biolink:object': 'A', 'biolink:predicate': 'biolink:related_to', 'biolink:source': source})
    g2.add_edge('E', 'D', **{'biolink:subject': 'E', 'biolink:object': 'D', 'biolink:predicate': 'biolink:subclass_of', 'biolink:source': source})
    g2.add_edge('F', 'D', **{'biolink:subject': 'F', 'biolink:object': 'D', 'biolink:predicate': 'biolink:subclass_of', 'biolink:source': source})

    g3 = NxGraph()
    g3.add_node('A', **{'biolink:id': 'A', 'biolink:category': ['biolink:NamedThing'], 'biolink:source': source})
    g3.add_node('B', **{'biolink:id': 'B', 'biolink:category': ['biolink:NamedThing'], 'biolink:source': source})
    g3.add_edge('A', 'B', **{'biolink:subject': 'A', 'biolink:object': 'B', 'biolink:predicate': 'biolink:related_to', 'biolink:source': source})

    g4 = NxGraph()
    g4.add_node('A', **{'biolink:id': 'A', 'biolink:category': ['biolink:Gene'], 'biolink:provided_by': source, 'biolink:source': source})
    g4.add_node('B', **{'biolink:id': 'B', 'biolink:category': ['biolink:Gene'], 'biolink:provided_by': source, 'biolink:source': source})
    g4.add_node('A1', **{'biolink:id': 'A1', 'biolink:category': ['biolink:Protein'], 'biolink:provided_by': source, 'biolink:source': source})
    g4.add_node('A2', **{'biolink:id': 'A2', 'biolink:category': ['biolink:Protein'], 'biolink:provided_by': source, 'biolink:source': source})
    g4.add_node('B1', **{'biolink:id': 'B1', 'biolink:category': ['biolink:Protein'], 'biolink:provided_by': source, 'biolink:source': source})
    g4.add_node('X', **{'biolink:id': 'X', 'biolink:category': ['biolink:Drug'], 'biolink:provided_by': source, 'biolink:source': source})
    g4.add_node('Y', **{'biolink:id': 'Y', 'biolink:category': ['biolink:Drug'], 'biolink:provided_by': source, 'biolink:source': source})
    g4.add_edge('A', 'A1', **{'biolink:subject': 'A', 'biolink:object': 'A1', 'biolink:predicate': 'biolink:has_gene_product', 'biolink:provided_by': source, 'biolink:source': source})
    g4.add_edge('A', 'A2', **{'biolink:subject': 'A', 'biolink:object': 'A2', 'biolink:predicate': 'biolink:has_gene_product', 'biolink:provided_by': source, 'biolink:source': source})
    g4.add_edge('B', 'B1', **{'biolink:subject': 'B', 'biolink:object': 'B1', 'biolink:predicate': 'biolink:has_gene_product', 'biolink:provided_by': source, 'biolink:source': source})
    g4.add_edge('X', 'A1', **{'biolink:subject': 'X', 'biolink:object': 'A1', 'biolink:predicate': 'biolink:interacts_with', 'biolink:provided_by': source, 'biolink:source': source})
    g4.add_edge('Y', 'B', **{'biolink:subject': 'Y', 'biolink:object': 'B', 'biolink:predicate': 'biolink:interacts_with', 'biolink:provided_by': source, 'biolink:source': source})
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

    t.graph.add_node('B', **{'biolink:id': 'B', 'biolink:publications': ['PMID:1', 'PMID:2'], 'biolink:category': ['biolink:NamedThing']})
    t.graph.add_node('C', **{'biolink:id': 'C', 'biolink:source': 'kgx-unit-test'})
    t.graph.add_edge('A', 'B', **{'biolink:subject': 'A', 'biolink:object': 'B', 'biolink:predicate': 'biolink:related_to', 'test_prop': 'VAL123'})
    assert t.graph.number_of_nodes() == 3
    t.save()

    nr = t.http_driver.query("MATCH (n) RETURN n")
    for node in nr:
        data = node[0]['data']
        if data['biolink:id'] == 'B':
            assert 'biolink:category' in data and data['biolink:category'] == ['biolink:NamedThing']
            assert 'biolink:publications' in data and data['biolink:publications'] == ['PMID:1', 'PMID:2']

    er = t.http_driver.query("MATCH ()-[p]-() RETURN p", data_contents=True, returns=(Node, Relationship, Node))
    for edge in er:
        data = edge[0].properties
        # assert data['id'] == 'A-biolink:related_to-B'
        assert data['biolink:subject'] == 'A'
        assert data['biolink:object'] == 'B'
        assert data['biolink:predicate'] == 'biolink:related_to'
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
    node_ids = [x['biolink:id'] for x in nodes]
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
