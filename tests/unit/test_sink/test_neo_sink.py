import pytest
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient.query import CypherException

from kgx.sink import NeoSink
from tests import clean_slate, DEFAULT_NEO4J_URL, DEFAULT_NEO4J_USERNAME, DEFAULT_NEO4J_PASSWORD
from tests.unit.test_sink import get_graph


@pytest.mark.skip()
def test_write_neo1(clean_slate):
    """
    Write a graph to a Neo4j instance using NeoSink.
    """
    graph = get_graph()
    s = NeoSink(
        uri=DEFAULT_NEO4J_URL,
        username=DEFAULT_NEO4J_USERNAME,
        password=DEFAULT_NEO4J_PASSWORD
    )
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()

    d = GraphDatabase(DEFAULT_NEO4J_URL, username=DEFAULT_NEO4J_USERNAME, password=DEFAULT_NEO4J_PASSWORD)

    try:
        results = d.query('MATCH (n) RETURN COUNT(*)')
        number_of_nodes = results[0][0]
        assert number_of_nodes == 6
    except CypherException as ce:
        print(ce)

    try:
        results = d.query('MATCH (s)-->(o) RETURN COUNT(*)')
        number_of_edges = results[0][0]
        assert number_of_edges == 6
    except CypherException as ce:
        print(ce)

