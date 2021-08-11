from kgx.graph.nx_graph import NxGraph


def get_graph():
    graph = NxGraph()
    graph.add_node(
        "A", **{"id": "A", "name": "Node A", "category": ["biolink:NamedThing"]}
    )
    graph.add_node(
        "B", **{"id": "B", "name": "Node B", "category": ["biolink:NamedThing"]}
    )
    graph.add_node(
        "C", **{"id": "C", "name": "Node C", "category": ["biolink:NamedThing"]}
    )
    graph.add_node(
        "D", **{"id": "D", "name": "Node D", "category": ["biolink:NamedThing"]}
    )
    graph.add_node(
        "E", **{"id": "E", "name": "Node E", "category": ["biolink:NamedThing"]}
    )
    graph.add_node(
        "F", **{"id": "F", "name": "Node F", "category": ["biolink:NamedThing"]}
    )
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
    return graph
