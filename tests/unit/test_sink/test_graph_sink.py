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
