from kgx.graph.nx_graph import NxGraph
from kgx.graph_operations.graph_merge import (
    merge_all_graphs,
    merge_graphs,
    merge_node,
    merge_edge,
)


def get_graphs():
    """
    Returns instances of defined graphs.
    """
    g1 = NxGraph()
    g1.name = "Graph 1"
    g1.add_node("A", id="A", name="Node A", category=["biolink:NamedThing"])
    g1.add_node("B", id="B", name="Node B", category=["biolink:NamedThing"])
    g1.add_node("C", id="C", name="Node C", category=["biolink:NamedThing"])
    g1.add_edge(
        "C",
        "B",
        edge_key="C-biolink:subclass_of-B",
        edge_label="biolink:sub_class_of",
        relation="rdfs:subClassOf",
    )
    g1.add_edge(
        "B",
        "A",
        edge_key="B-biolink:subclass_of-A",
        edge_label="biolink:sub_class_of",
        relation="rdfs:subClassOf",
        provided_by="Graph 1",
    )

    g2 = NxGraph()
    g2.name = "Graph 2"
    g2.add_node(
        "A",
        id="A",
        name="Node A",
        description="Node A in Graph 2",
        category=["biolink:NamedThing"],
    )
    g2.add_node(
        "B",
        id="B",
        name="Node B",
        description="Node B in Graph 2",
        category=["biolink:NamedThing"],
    )
    g2.add_node(
        "C",
        id="C",
        name="Node C",
        description="Node C in Graph 2",
        category=["biolink:NamedThing"],
    )
    g2.add_node(
        "D",
        id="D",
        name="Node D",
        description="Node D in Graph 2",
        category=["biolink:NamedThing"],
    )
    g2.add_node(
        "E",
        id="E",
        name="Node E",
        description="Node E in Graph 2",
        category=["biolink:NamedThing"],
    )
    g2.add_edge(
        "B",
        "A",
        edge_key="B-biolink:subclass_of-A",
        edge_label="biolink:subclass_of",
        relation="rdfs:subClassOf",
        provided_by="Graph 2",
    )
    g2.add_edge(
        "B",
        "A",
        edge_key="B-biolink:related_to-A",
        edge_label="biolink:related_to",
        relation="biolink:related_to",
    )
    g2.add_edge(
        "D",
        "A",
        edge_key="D-biolink:related_to-A",
        edge_label="biolink:related_to",
        relation="biolink:related_to",
    )
    g2.add_edge(
        "E",
        "A",
        edge_key="E-biolink:related_to-A",
        edge_label="biolink:related_to",
        relation="biolink:related_to",
    )

    g3 = NxGraph()
    g3.name = "Graph 3"
    g3.add_edge(
        "F",
        "E",
        edge_key="F-biolink:same_as-E",
        edge_label="biolink:same_as",
        relation="OWL:same_as",
    )

    return [g1, g2, g3]


def test_merge_all_graphs():
    """
    Test for merging three graphs into one,
    while preserving conflicting node and edge properties.
    """
    graphs = get_graphs()
    # merge while preserving conflicting nodes and edges
    merged_graph = merge_all_graphs(graphs, preserve=True)
    assert merged_graph.number_of_nodes() == 6
    assert merged_graph.number_of_edges() == 6
    assert merged_graph.name == "Graph 2"

    data = merged_graph.nodes()["A"]
    assert data["name"] == "Node A"
    assert data["description"] == "Node A in Graph 2"

    edges = merged_graph.get_edge("B", "A")
    assert len(edges) == 2

    data = list(edges.values())[0]
    assert len(data["provided_by"]) == 2
    assert data["provided_by"] == ["Graph 2", "Graph 1"]

    graphs = get_graphs()
    # merge while not preserving conflicting nodes and edges
    merged_graph = merge_all_graphs(graphs, preserve=False)
    assert merged_graph.number_of_nodes() == 6
    assert merged_graph.number_of_edges() == 6
    assert merged_graph.name == "Graph 2"

    data = merged_graph.nodes()["A"]
    assert data["name"] == "Node A"
    assert data["description"] == "Node A in Graph 2"

    edges = merged_graph.get_edge("B", "A")
    assert len(edges) == 2

    data = list(edges.values())[0]
    assert isinstance(data["provided_by"], list)
    assert "Graph 1" in data["provided_by"]
    assert "Graph 2" in data["provided_by"]


def test_merge_graphs():
    """
    Test for merging 3 graphs into one,
    while not preserving conflicting node and edge properties.
    """
    graphs = get_graphs()
    merged_graph = merge_graphs(NxGraph(), graphs)
    assert merged_graph.number_of_nodes() == 6
    assert merged_graph.number_of_edges() == 6
    assert merged_graph.name not in [x.name for x in graphs]


def test_merge_node():
    """
    Test merging of a node into a graph.
    """
    graphs = get_graphs()
    g = graphs[0]
    node = g.nodes()["A"]
    new_data = node.copy()
    new_data["subset"] = "test"
    new_data["source"] = "KGX"
    new_data["category"] = ["biolink:InformationContentEntity"]
    new_data["description"] = "Node A modified by merge operation"
    node = merge_node(g, node["id"], new_data, preserve=True)

    assert node["id"] == "A"
    assert node["name"] == "Node A"
    assert node["description"] == "Node A modified by merge operation"
    assert "subset" in node and node["subset"] == "test"
    assert "source" in node and node["source"] == "KGX"


def test_merge_edge():
    """
    Test merging of an edge into a graph.
    """
    graphs = get_graphs()
    g = graphs[1]
    edge = g.get_edge("E", "A")
    new_data = edge.copy()
    new_data["provided_by"] = "KGX"
    new_data["evidence"] = "PMID:123456"
    edge = merge_edge(g, "E", "A", "E-biolink:related_to-A", new_data, preserve=True)

    assert edge["edge_label"] == "biolink:related_to"
    assert edge["relation"] == "biolink:related_to"
    assert "KGX" in edge["provided_by"]
    assert edge["evidence"] == "PMID:123456"
