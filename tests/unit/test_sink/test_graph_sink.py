from kgx.sink import GraphSink


def test_write_graph():
    """
    Write a graph via GraphSink.
    """
    s = GraphSink()
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
