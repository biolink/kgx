import os

from kgx.graph.nx_graph import NxGraph
from kgx.sink import TsvSink
from kgx.transformer import Transformer
from tests import TARGET_DIR


def test_write_tsv1():
    """
    Write a graph to a TSV file using TsvSink.
    """
    graph = NxGraph()
    graph.add_node("A", id="A", **{"name": "Node A", "category": ["biolink:NamedThing", "biolink:Gene"]})
    graph.add_node("B", id="B", **{"name": "Node B", "category": ["biolink:NamedThing"]})
    graph.add_node("C", id="C", **{"name": "Node C", "category": ["biolink:NamedThing"]})
    graph.add_node("D", id="D", **{"name": "Node D", "category": ["biolink:NamedThing"]})
    graph.add_node("E", id="E", **{"name": "Node E", "category": ["biolink:NamedThing"]})
    graph.add_node("F", id="F", **{"name": "Node F", "category": ["biolink:NamedThing"]})
    graph.add_edge(
        "B", "A", **{"subject": "B", "object": "A", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "C", "B", **{"subject": "C", "object": "B", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "D", "C", **{"subject": "D", "object": "C", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "D", "A", **{"subject": "D", "object": "A", "predicate": "biolink:related_to"}
    )
    graph.add_edge(
        "E", "D", **{"subject": "E", "object": "D", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "F", "D", **{"subject": "F", "object": "D", "predicate": "biolink:sub_class_of"}
    )

    t = Transformer()
    s = TsvSink(
        owner=t,
        filename=os.path.join(TARGET_DIR, "test_graph"),
        format="tsv",
        node_properties={"id", "name", "category"},
        edge_properties={"subject", "predicate", "object", "relation"},
    )
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()

    node_lines = open(os.path.join(TARGET_DIR, "test_graph_nodes.tsv")).readlines()
    edge_lines = open(os.path.join(TARGET_DIR, "test_graph_edges.tsv")).readlines()
    assert len(node_lines) == 7
    assert len(edge_lines) == 7

    for n in node_lines:
        assert len(n.split("\t")) == 3
    for e in edge_lines:
        assert len(e.split("\t")) == 4


def test_write_tsv2():
    """
    Write a graph to a TSV archive using TsvSink.
    """
    graph = NxGraph()
    graph.add_node("A", id="A", **{"name": "Node A", "category": ["biolink:NamedThing", "biolink:Gene"]})
    graph.add_node("B", id="B", **{"name": "Node B"})
    graph.add_node("C", id="C", **{"name": "Node C"})
    graph.add_node("D", id="D", **{"name": "Node D"})
    graph.add_node("E", id="E", **{"name": "Node E"})
    graph.add_node("F", id="F", **{"name": "Node F"})
    graph.add_edge(
        "B", "A", **{"subject": "B", "object": "A", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "C", "B", **{"subject": "C", "object": "B", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "D", "C", **{"subject": "D", "object": "C", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "D", "A", **{"subject": "D", "object": "A", "predicate": "biolink:related_to"}
    )
    graph.add_edge(
        "E", "D", **{"subject": "E", "object": "D", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "F", "D", **{"subject": "F", "object": "D", "predicate": "biolink:sub_class_of"}
    )

    t = Transformer()
    s = TsvSink(
        owner=t,
        filename=os.path.join(TARGET_DIR, "test_graph_archive"),
        format="tsv",
        compression="tar",
        node_properties={"id", "name"},
        edge_properties={"subject", "predicate", "object", "relation"},
    )
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()

    assert os.path.exists(os.path.join(TARGET_DIR, "test_graph_archive.tar"))


def test_write_tsv3():
    """
        Write a graph to a TSV archive using TsvSink.
    """
    graph = NxGraph()
    graph.add_node("A", id="A", **{"name": "Node A", "category": ["biolink:NamedThing", "biolink:Gene"]})
    graph.add_node("B", id="B", **{"name": "Node B"})
    graph.add_node("C", id="C", **{"name": "Node C"})
    graph.add_node("D", id="D", **{"name": "Node D"})
    graph.add_node("E", id="E", **{"name": "Node E"})
    graph.add_node("F", id="F", **{"name": "Node F"})
    graph.add_edge(
        "B", "A", **{"subject": "B", "object": "A", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "C", "B", **{"subject": "C", "object": "B", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "D", "C", **{"subject": "D", "object": "C", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "D", "A", **{"subject": "D", "object": "A", "predicate": "biolink:related_to"}
    )
    graph.add_edge(
        "E", "D", **{"subject": "E", "object": "D", "predicate": "biolink:sub_class_of"}
    )
    graph.add_edge(
        "F", "D", **{"subject": "F", "object": "D", "predicate": "biolink:sub_class_of"}
    )
    t = Transformer()
    s = TsvSink(
        owner=t,
        filename=os.path.join(TARGET_DIR, "test_graph_archive"),
        format="tsv",
        compression="tar.gz",
        node_properties={"id", "name"},
        edge_properties={"subject", "predicate", "object", "relation"},
    )
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()

    assert os.path.exists(os.path.join(TARGET_DIR, "test_graph_archive.tar.gz"))