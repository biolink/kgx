from kgx.sink import GraphSink
from kgx.transformer import Transformer


def test_write_graph_no_edge_identifier():
    """
    Write a graph via GraphSink.
    """
    t = Transformer()
    s = GraphSink(t)
    s.write_node({"id": "A", "name": "Node A", "category": ["biolink:NamedThing"]})
    s.write_node({"id": "B", "name": "Node B", "category": ["biolink:NamedThing"]})
    s.write_node({"id": "C", "name": "Node C", "category": ["biolink:NamedThing"]})
    s.write_edge(
        {
            "subject": "A",
            "predicate": "biolink:related_to",
            "object": "B",
            "relation": "biolink:related_to",
        }
    )

    assert s.graph.number_of_nodes() == 3
    assert s.graph.number_of_edges() == 1


def test_write_graph_edge_key():
    """
    Write parallel edges that share subject, predicate and object but carry
    distinct keys via GraphSink.
    """
    t = Transformer()
    s = GraphSink(t)
    s.write_node({"id": "A", "name": "Node A", "category": ["biolink:NamedThing"]})
    s.write_node({"id": "B", "name": "Node B", "category": ["biolink:NamedThing"]})
    for key, label in [("e1", "SYNAPSED_BY"), ("e2", "SYNAPSED_TO")]:
        s.write_edge(
            {
                "key": key,
                "subject": "A",
                "predicate": "biolink:related_to",
                "object": "B",
                "Label": label,
            }
        )

    assert s.graph.number_of_edges() == 2
    assert s.graph.get_edge("A", "B", "e1")["Label"] == "SYNAPSED_BY"
    assert s.graph.get_edge("A", "B", "e2")["Label"] == "SYNAPSED_TO"
    assert "key" not in s.graph.get_edge("A", "B", "e1")
