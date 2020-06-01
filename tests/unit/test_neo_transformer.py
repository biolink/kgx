import logging
from os import wait
from time import sleep

import pytest
import docker
from docker.errors import APIError
from neo4jrestclient.client import GraphDatabase, Node, Relationship
from networkx import MultiDiGraph

from kgx import NeoTransformer


@pytest.fixture(scope='module')
def setup_neo4j():
    container_name = 'kgx-neo4j-unit-test2'
    c = start_container(container_name)
    sleep(10)
    yield c
    stop_container(container_name)


@pytest.fixture(scope='function')
def clean_slate(source='kgx-unit-test'):
    # TODO: make this configurable
    http_driver = GraphDatabase('http://localhost:7474', username='neo4j', password='test')
    q = "MATCH (n { source : '" + source + "' }) DETACH DELETE (n)"
    print(q)
    http_driver.query(q)


def start_container(name):
    client = docker.from_env()
    create_container = False
    try:
        c = client.containers.get(name)
        logging.debug(f"Container '{name}' already exists")
        if c.status == 'exited':
            logging.debug(f"Restarting container '{name}'...")
            c.restart()
    except:
        create_container = True

    if create_container:
        logging.debug(f"Creating container '{name}'")
        try:
            c = client.containers.run(
                'neo4j:3.5.3', name=name, detach=True,
                environment=['NEO4J_AUTH=neo4j/test'],
                ports={'7687': '7687', '7474': '7474'}
            )
        except APIError as e:
            logging.error(e)
    return c


def stop_container(name):
    client = docker.from_env()
    try:
        c = client.containers.get(name)
        c.stop()
    except:
        logging.error(f"'{name}' container not found")


def get_graph(source):
    g1 = MultiDiGraph()
    g1.name = 'Graph 1'
    g1.add_node('A', id='A', name='Node A', category=['biolink:NamedThing'], source=source)
    g1.add_node('B', id='B', name='Node B', category=['biolink:NamedThing'], source=source)
    g1.add_node('C', id='C', name='Node C', category=['biolink:NamedThing'], source=source)
    g1.add_edge('B', 'A', subject='B', object='A', edge_label='biolink:sub_class_of', source=source)

    g2 = MultiDiGraph()
    g2.add_node('A', id='A', source=source)
    g2.add_node('B', id='B', source=source)
    g2.add_node('C', id='C', source=source)
    g2.add_node('D', id='D', source=source)
    g2.add_node('E', id='E', source=source)
    g2.add_node('F', id='F', source=source)
    g2.add_edge('B', 'A', subject='B', object='A', edge_label='biolink:sub_class_of', source=source)
    g2.add_edge('C', 'B', subject='C', object='B',  edge_label='biolink:sub_class_of', source=source)
    g2.add_edge('D', 'C', subject='D', object='C',  edge_label='biolink:sub_class_of', source=source)
    g2.add_edge('D', 'A', subject='D', object='A',  edge_label='biolink:related_to', source=source)
    g2.add_edge('E', 'D', subject='E', object='D',  edge_label='biolink:sub_class_of', source=source)
    g2.add_edge('F', 'D', subject='F', object='D',  edge_label='biolink:sub_class_of', source=source)

    g3 = MultiDiGraph()
    g3.add_node('A', id='A', category=['biolink:NamedThing'], source=source)
    g3.add_node('B', id='B', category=['biolink:NamedThing'], source=source)
    g3.add_edge('A', 'B', subject='A', object='B', edge_label='biolink:related_to', source=source)

    return [g1, g2, g3]


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


@pytest.mark.parametrize('query', [
    (get_graph('kgx-unit-test')[0], 3, 1),
    (get_graph('kgx-unit-test')[1], 6, 6)
])
def test_load(setup_neo4j, clean_slate, query):
    t = NeoTransformer(query[0], uri='http://localhost:7474', username='neo4j', password='test')
    t.save()

    nr = t.http_driver.query("MATCH (n) RETURN count(n)")
    [node_counts] = [x for x in nr][0]
    assert node_counts == query[1]

    er = t.http_driver.query("MATCH ()-[p]->() RETURN count(p)")
    [edge_counts] = [x for x in er][0]
    assert edge_counts == query[2]


def test_save_merge(setup_neo4j, clean_slate):
    g = get_graph('kgx-unit-test')[2]
    t = NeoTransformer(g, uri='http://localhost:7474', username='neo4j', password='test')
    t.save()

    t.graph.add_node('B', id='B', publications=['PMID:1', 'PMID:2'], category=['biolink:NamedThing'])
    t.graph.add_node('C', id='C', source='kgx-unit-test')
    t.graph.add_edge('A', 'B', subject='A', object='B', edge_label='biolink:related_to', test_prop='VAL123')
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
        print(edge)
        data = edge[0].properties
        # assert data['id'] == 'A-biolink:related_to-B'
        assert data['subject'] == 'A'
        assert data['object'] == 'B'
        assert data['edge_label'] == 'biolink:related_to'
        assert data['test_prop'] == 'VAL123'


@pytest.mark.skip('After implementing filters')
def test_get_pages():
    pass


@pytest.mark.skip('After implementing filters')
def test_get_nodes():
    pass


@pytest.mark.skip('After implementing filters')
def test_get_edges():
    pass

