import pytest

from kgx.sink import ArangoSink
from kgx.transformer import Transformer
from tests import print_graph
from tests.unit import (
    clean_arango_database,
    DEFAULT_ARANGO_URL,
    DEFAULT_ARANGO_USERNAME,
    DEFAULT_ARANGO_PASSWORD,
    DEFAULT_ARANGO_DATABASE,
    check_arango_container,
    ARANGO_CONTAINER_NAME,
)
from tests.unit import get_graph


def test_sanitize_key():
    """
    Test to ensure behavior of _sanitize_key.
    """
    assert ArangoSink._sanitize_key("CURIE:12345") == "CURIE:12345"
    assert ArangoSink._sanitize_key("PREFIX/suffix") == "PREFIX_suffix"
    assert ArangoSink._sanitize_key("a/b/c") == "a_b_c"
    assert ArangoSink._sanitize_key("simple") == "simple"


@pytest.mark.skipif(
    not check_arango_container(),
    reason=f"Container {ARANGO_CONTAINER_NAME} is not running",
)
def test_write_arango1(clean_arango_database):
    """
    Write a graph to an ArangoDB instance using ArangoSink.
    """
    graph = get_graph("test")[0]
    t = Transformer()
    s = ArangoSink(
        owner=t,
        uri=DEFAULT_ARANGO_URL,
        database=DEFAULT_ARANGO_DATABASE,
        username=DEFAULT_ARANGO_USERNAME,
        password=DEFAULT_ARANGO_PASSWORD,
        cache_size=1000,
    )
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()

    from arango import ArangoClient

    client = ArangoClient(hosts=DEFAULT_ARANGO_URL)
    db = client.db(
        DEFAULT_ARANGO_DATABASE,
        username=DEFAULT_ARANGO_USERNAME,
        password=DEFAULT_ARANGO_PASSWORD,
    )

    nodes_col = db.collection("nodes")
    edges_col = db.collection("edges")

    assert nodes_col.count() == 3
    assert edges_col.count() == 1


@pytest.mark.skipif(
    not check_arango_container(),
    reason=f"Container {ARANGO_CONTAINER_NAME} is not running",
)
@pytest.mark.parametrize(
    "query",
    [(get_graph("kgx-unit-test")[0], 3, 1), (get_graph("kgx-unit-test")[1], 6, 6)],
)
def test_write_arango2(clean_arango_database, query):
    """
    Test writing a graph to an ArangoDB instance.
    """
    graph = query[0]
    t = Transformer()
    sink = ArangoSink(
        owner=t,
        uri=DEFAULT_ARANGO_URL,
        database=DEFAULT_ARANGO_DATABASE,
        username=DEFAULT_ARANGO_USERNAME,
        password=DEFAULT_ARANGO_PASSWORD,
    )
    for n, data in graph.nodes(data=True):
        sink.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        sink.write_edge(data)
    sink.finalize()

    from arango import ArangoClient

    client = ArangoClient(hosts=DEFAULT_ARANGO_URL)
    db = client.db(
        DEFAULT_ARANGO_DATABASE,
        username=DEFAULT_ARANGO_USERNAME,
        password=DEFAULT_ARANGO_PASSWORD,
    )

    nodes_col = db.collection("nodes")
    edges_col = db.collection("edges")

    assert nodes_col.count() >= query[1]
    assert edges_col.count() >= query[2]


@pytest.mark.skipif(
    not check_arango_container(),
    reason=f"Container {ARANGO_CONTAINER_NAME} is not running",
)
def test_write_arango3(clean_arango_database):
    """
    Test writing a graph and then writing a slightly
    modified version of the graph to the same ArangoDB instance.
    """
    graph = get_graph("kgx-unit-test")[2]
    t = Transformer()
    sink = ArangoSink(
        owner=t,
        uri=DEFAULT_ARANGO_URL,
        database=DEFAULT_ARANGO_DATABASE,
        username=DEFAULT_ARANGO_USERNAME,
        password=DEFAULT_ARANGO_PASSWORD,
    )
    for n, data in graph.nodes(data=True):
        sink.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        sink.write_edge(data)
    sink.finalize()

    graph.add_node(
        "B",
        id="B",
        publications=["PMID:1", "PMID:2"],
        category=["biolink:NamedThing"],
    )
    graph.add_node(
        "C", id="C", category=["biolink:NamedThing"], source="kgx-unit-test"
    )
    e = graph.get_edge("A", "B")
    edge_key = list(e.keys())[0]
    graph.add_edge_attribute(
        "A", "B", edge_key, attr_key="test_prop", attr_value="VAL123"
    )
    print_graph(graph)
    assert graph.number_of_nodes() == 3
    assert graph.number_of_edges() == 1

    # Write the modified graph (upsert behavior)
    for n, data in graph.nodes(data=True):
        sink.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        sink.write_edge(data)
    sink.finalize()

    from arango import ArangoClient

    client = ArangoClient(hosts=DEFAULT_ARANGO_URL)
    db = client.db(
        DEFAULT_ARANGO_DATABASE,
        username=DEFAULT_ARANGO_USERNAME,
        password=DEFAULT_ARANGO_PASSWORD,
    )

    nodes_col = db.collection("nodes")
    edges_col = db.collection("edges")

    # Should still have 3 nodes (upsert, not duplicate)
    assert nodes_col.count() == 3
    assert edges_col.count() == 1
