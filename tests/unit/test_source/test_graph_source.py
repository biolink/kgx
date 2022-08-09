from kgx.graph.nx_graph import NxGraph
from kgx.source import GraphSource
from kgx.transformer import Transformer


def test_read_graph1():
    """
    Read from an NxGraph using GraphSource.
    """
    graph = NxGraph()
    graph.add_node("A", **{"id": "A", "name": "node A"})
    graph.add_node("B", **{"id": "B", "name": "node B"})
    graph.add_node("C", **{"id": "C", "name": "node C"})
    graph.add_edge(
        "A",
        "C",
        **{
            "subject": "A",
            "predicate": "biolink:related_to",
            "object": "C",
            "relation": "biolink:related_to",
        }
    )
    t = Transformer()
    s = GraphSource(t)

    g = s.parse(graph=graph)
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1], rec[2])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    assert len(nodes.keys()) == 3
    n1 = nodes["A"]
    assert n1["id"] == "A"
    assert n1["name"] == "node A"

    assert len(edges.keys()) == 1
    e1 = list(edges.values())[0]
    assert e1["subject"] == "A"
    assert e1["predicate"] == "biolink:related_to"
    assert e1["object"] == "C"
    assert e1["relation"] == "biolink:related_to"


def test_read_graph2():
    """
    Read from an NxGraph using GraphSource.
    This test also supplies the provided_by parameter.
    """
    graph = NxGraph()
    graph.add_node("A", **{"id": "A", "name": "node A"})
    graph.add_node("B", **{"id": "B", "name": "node B"})
    graph.add_node("C", **{"id": "C", "name": "node C"})
    graph.add_edge(
        "A",
        "C",
        **{
            "subject": "A",
            "predicate": "biolink:related_to",
            "object": "C",
            "relation": "biolink:related_to",
        }
    )
    t = Transformer()
    s = GraphSource(t)

    g = s.parse(graph=graph, provided_by="Test Graph", knowledge_source="Test Graph")
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1], rec[2])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    assert len(nodes.keys()) == 3
    n1 = nodes["A"]
    assert n1["id"] == "A"
    assert n1["name"] == "node A"
    print("n1:", n1)
    assert "Test Graph" in n1["provided_by"]

    assert len(edges.keys()) == 1
    e1 = list(edges.values())[0]
    assert e1["subject"] == "A"
    assert e1["predicate"] == "biolink:related_to"
    assert e1["object"] == "C"
    assert e1["relation"] == "biolink:related_to"
    print("e1:", e1)
    assert "Test Graph" in e1["knowledge_source"]
