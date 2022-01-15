import pytest
from neo4j import GraphDatabase

from kgx.source import NeoSource
from kgx.transformer import Transformer
from tests.unit import (
    clean_database,
    DEFAULT_NEO4J_URL,
    DEFAULT_NEO4J_USERNAME,
    DEFAULT_NEO4J_PASSWORD,
    load_graph_dictionary,
    check_container,
    CONTAINER_NAME
)


queries = [
    "CREATE (n:`biolink:NamedThing` {id: 'A', name: 'A', category: ['biolink:NamedThing']})",
    "CREATE (n:`biolink:NamedThing` {id: 'B', name: 'B', category: ['biolink:NamedThing']})",
    "CREATE (n:`biolink:NamedThing` {id: 'C', name: 'C', category: ['biolink:NamedThing']})",
    """
    MATCH (s), (o)
    WHERE s.id = 'A' AND o.id = 'B'
    CREATE (s)-[p:`biolink:related_to` {subject: s.id, object: o.id, predicate: 'biolink:related_to', relation: 'biolink:related_to'}]->(o)
    RETURN p
    """,
    """
    MATCH (s), (o)
    WHERE s.id = 'A' AND o.id = 'C'
    CREATE (s)-[p:`biolink:related_to` {subject: s.id, object: o.id, predicate: 'biolink:related_to', relation: 'biolink:related_to'}]->(o)
    RETURN p
    """,
]


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_read_neo(clean_database):
    """
    Read a graph from a Neo4j instance.
    """
    with GraphDatabase.driver(
            DEFAULT_NEO4J_URL,
            auth=(DEFAULT_NEO4J_USERNAME, DEFAULT_NEO4J_PASSWORD)
    ) as http_driver:
        session = http_driver.session()
        for q in queries:
            session.run(q)

    t = Transformer()
    s = NeoSource(t)

    g = s.parse(
        uri=DEFAULT_NEO4J_URL,
        username=DEFAULT_NEO4J_USERNAME,
        password=DEFAULT_NEO4J_PASSWORD,
    )

    nodes, edges = load_graph_dictionary(g)

    count = s.count()
    assert count > 0
    assert len(nodes.keys()) == 3
    assert len(edges.keys()) == 2

    n1 = nodes["A"]
    assert n1["id"] == "A"
    assert n1["name"] == "A"
    assert "category" in n1 and "biolink:NamedThing" in n1["category"]

    e1 = edges[("A", "C")][0]
    assert e1["subject"] == "A"
    assert e1["object"] == "C"
    assert e1["predicate"] == "biolink:related_to"
    assert e1["relation"] == "biolink:related_to"
