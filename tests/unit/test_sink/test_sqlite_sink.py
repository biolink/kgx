import os

from kgx.graph.nx_graph import NxGraph
from kgx.sink import SqlSink
from kgx.transformer import Transformer
from tests import TARGET_DIR
from kgx.utils.kgx_utils import create_connection, drop_existing_tables

NAMED_THING = "biolink:NamedThing"
SUBCLASS_OF = "biolink:sub_class_of"


def test_write_sqlite():
    """
    Write a graph to a sqlite db file using SqlSink.
    """
    conn = create_connection(os.path.join(TARGET_DIR, "test_graph.db"))
    drop_existing_tables(conn)

    graph = NxGraph()
    graph.add_node("A", id="A", **{"name": "Node A", "category": [NAMED_THING, "biolink:Gene"]})
    graph.add_node("B", id="B", **{"name": "Node B", "category": [NAMED_THING]})
    graph.add_node("C", id="C", **{"name": "Node C", "category": [NAMED_THING]})
    graph.add_node("D", id="D", **{"name": "Node D", "category": [NAMED_THING]})
    graph.add_node("E", id="E", **{"name": "Node E", "category": [NAMED_THING]})
    graph.add_node("F", id="F", **{"name": "Node F", "category": [NAMED_THING]})
    graph.add_edge(
        "B", "A", **{"subject": "B", "object": "A", "predicate": SUBCLASS_OF}
    )
    graph.add_edge(
        "C", "B", **{"subject": "C", "object": "B", "predicate": SUBCLASS_OF}
    )
    graph.add_edge(
        "D", "C", **{"subject": "D", "object": "C", "predicate": SUBCLASS_OF}
    )
    graph.add_edge(
        "D", "A", **{"subject": "D", "object": "A", "predicate": "biolink:related_to"}
    )
    graph.add_edge(
        "E", "D", **{"subject": "E", "object": "D", "predicate": SUBCLASS_OF}
    )
    graph.add_edge(
        "F", "D", **{"subject": "F", "object": "D", "predicate": SUBCLASS_OF}
    )

    t = Transformer()
    s = SqlSink(
        owner=t,
        filename=os.path.join(TARGET_DIR, "test_graph.db"),
        format="sql",
        node_properties={"id", "name", "category"},
        edge_properties={"subject", "predicate", "object", "relation"},
    )
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()

    cur = conn.cursor()
    sql_query = """SELECT name FROM sqlite_master  
      WHERE type='table';"""
    cur.execute(sql_query)
    tables = cur.fetchall()
    assert len(tables) == 2
    cur.execute("SELECT count(*) FROM nodes")
    number_of_nodes = cur.fetchone()[0]
    assert number_of_nodes == 6

    cur.execute("SELECT count(*) FROM edges")
    number_of_edges = cur.fetchone()[0]
    assert number_of_edges == 6


def test_write_denormalized_sqlite():
    """
    Write a graph to a sqlite db file using SqlSink.
    """
    conn = create_connection(os.path.join(TARGET_DIR, "test_graph.db"))
    drop_existing_tables(conn)

    graph = NxGraph()
    graph.add_node("A", id="A", **{"name": "Node A", "category": [NAMED_THING, "biolink:Gene"]})
    graph.add_node("B", id="B", **{"name": "Node B", "category": [NAMED_THING]})
    graph.add_node("C", id="C", **{"name": "Node C", "category": [NAMED_THING]})
    graph.add_node("D", id="D", **{"name": "Node D", "category": [NAMED_THING]})
    graph.add_node("E", id="E", **{"name": "Node E", "category": [NAMED_THING]})
    graph.add_node("F", id="F", **{"name": "Node F", "category": [NAMED_THING]})
    graph.add_edge(
        "B", "A", **{"subject": "B", "object": "A", "predicate": SUBCLASS_OF}
    )
    graph.add_edge(
        "C", "B", **{"subject": "C", "object": "B", "predicate": SUBCLASS_OF}
    )
    graph.add_edge(
        "D", "C", **{"subject": "D", "object": "C", "predicate": SUBCLASS_OF}
    )
    graph.add_edge(
        "D", "A", **{"subject": "D", "object": "A", "predicate": "biolink:related_to"}
    )
    graph.add_edge(
        "E", "D", **{"subject": "E", "object": "D", "predicate": SUBCLASS_OF}
    )
    graph.add_edge(
        "F", "D", **{"subject": "F", "object": "D", "predicate": SUBCLASS_OF}
    )

    t = Transformer()
    s = SqlSink(
        owner=t,
        filename=os.path.join(TARGET_DIR, "test_graph.db"),
        format="sql",
        node_properties={"id", "name", "category"},
        edge_properties={"subject", "predicate", "object", "relation"},
        denormalize=True,
    )
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()

    cur = conn.cursor()
    sql_query = """SELECT name FROM sqlite_master  
      WHERE type='table';"""
    cur.execute(sql_query)
    tables = cur.fetchall()
    assert len(tables) == 2

    cur.execute("SELECT count(*) FROM edges")
    number_of_edges = cur.fetchone()[0]
    assert number_of_edges == 6

