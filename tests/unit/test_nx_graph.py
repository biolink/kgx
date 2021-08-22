from kgx.graph.nx_graph import NxGraph


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


def test_add_node():
    """
    Test adding a node to an NxGraph.
    """
    g = NxGraph()
    g.add_node("A")
    g.add_node("A", name="Node A", description="Node A")
    assert g.has_node("A")


def test_add_edge():
    """
    Test adding an edge to an NxGraph.
    """
    g = NxGraph()
    g.add_node("A")
    g.add_node("B")
    g.add_edge("A", "B", predicate="biolink:related_to", provided_by="test")
    assert g.has_edge("A", "B")
    g.add_edge("B", "C", edge_key="B-biolink:related_to-C", provided_by="test")
    assert g.has_edge("B", "C")


def test_add_node_attribute():
    """
    Test adding a node attribute to an NxGraph.
    """
    g = NxGraph()
    g.add_node("A")
    g.add_node_attribute("A", "provided_by", "test")
    n = g.get_node("A")
    assert "provided_by" in n and n["provided_by"] == "test"


def test_add_edge_attribute():
    """
    Test adding an edge attribute to an NxGraph.
    """
    g = NxGraph()
    g.add_edge("A", "B")
    g.add_edge_attribute("A", "B", "edge_ab", "predicate", "biolink:related_to")


def test_update_node_attribute():
    """
    Test updating a node attribute for a node in an NxGraph.
    """
    g = NxGraph()
    g.add_node("A", name="A", description="Node A")
    g.update_node_attribute("A", "description", "Modified description")
    n = g.get_node("A")
    assert "name" in n and n["name"] == "A"
    assert "description" in n and n["description"] == "Modified description"


def test_update_edge_attribute():
    """
    Test updating an edge attribute for an edge in an NxGraph.
    """
    g = NxGraph()
    g.add_edge("A", "B", "edge_ab")
    g.update_edge_attribute("A", "B", "edge_ab", "source", "test")
    e = g.get_edge("A", "B", "edge_ab")
    assert "source" in e and e["source"] == "test"


def test_nodes():
    """
    Test fetching of nodes from an NxGraph.
    """
    g = get_graphs()[0]
    nodes = list(g.nodes(data=False))
    assert len(nodes) == 3
    assert nodes[0] == "A"

    nodes = g.nodes(data=True)
    assert len(nodes) == 3
    assert "name" in nodes["A"] and nodes["A"]["name"] == "Node A"


def test_edges():
    """
    Test fetching of edges from an NxGraph.
    """
    g = get_graphs()[0]
    edges = list(g.edges(keys=False, data=False))
    assert len(edges) == 2
    assert edges[0] == ("B", "A")

    edges = list(g.edges(keys=False, data=True))
    e1 = edges[0]
    assert e1[0] == "B"
    assert e1[1] == "A"
    assert e1[2]["relation"] == "rdfs:subClassOf"

    edges = list(g.edges(keys=True, data=True))
    e1 = edges[0]
    assert e1[0] == "B"
    assert e1[1] == "A"
    assert e1[2] == "B-biolink:subclass_of-A"
    assert e1[3]["relation"] == "rdfs:subClassOf"


def test_in_edges():
    """
    Test fetching of incoming edges for a node in an NxGraph.
    """
    g = get_graphs()[1]
    in_edges = list(g.in_edges("A", keys=False, data=False))
    assert len(in_edges) == 3
    assert in_edges[0] == ("B", "A")

    in_edges = list(g.in_edges("A", keys=True, data=True))
    e1 = in_edges[0]
    assert e1
    assert e1[0] == "B"
    assert e1[1] == "A"
    assert e1[2] == "B-biolink:related_to-A"
    assert e1[3]["relation"] == "biolink:related_to"


def test_out_edges():
    """
    Test fetching of outgoing edges for a node in an NxGraph.
    """
    g = get_graphs()[1]
    out_edges = list(g.out_edges("B", keys=False, data=False))
    assert len(out_edges) == 1
    assert out_edges[0] == ("B", "A")

    out_edges = list(g.out_edges("B", keys=True, data=True))
    e1 = out_edges[0]
    assert e1
    assert e1[0] == "B"
    assert e1[1] == "A"
    assert e1[2] == "B-biolink:related_to-A"
    assert e1[3]["relation"] == "biolink:related_to"


def test_nodes_iter():
    """
    Test fetching all nodes in an NxGraph via an iterator.
    """
    g = get_graphs()[1]
    n_iter = g.nodes_iter()
    n = next(n_iter)
    assert n[1]["id"] == "A"
    assert n[1]["name"] == "Node A"


def test_edges_iter():
    """
    Test fetching all edges in an NxGraph via an iterator.
    """
    g = get_graphs()[1]
    e_iter = g.edges_iter()
    e = next(e_iter)
    assert len(e) == 4
    assert e[0] == "B"
    assert e[1] == "A"
    assert e[2] == "B-biolink:related_to-A"
    assert e[3]["relation"] == "biolink:related_to"


def test_remove_node():
    """
    Test removing a node from an NxGraph.
    """
    g = get_graphs()[1]
    g.remove_node("A")
    assert not g.has_node("A")


def test_remove_edge():
    """
    Test removing an edge from an NxGraph.
    """
    g = get_graphs()[1]
    g.remove_edge("B", "A")
    assert not g.has_edge("B", "A")


def test_number_of_nodes_edges():
    """
    Test getting number of nodes and edges in an NxGraph.
    """
    g = get_graphs()[1]
    assert g.number_of_nodes() == 5
    assert g.number_of_edges() == 3


def test_set_node_attributes():
    """
    Test setting node attributes in bulk.
    """
    g = NxGraph()
    g.add_node("X:1", alias="A:1")
    g.add_node("X:2", alias="B:2")
    d = {"X:1": {"alias": "ABC:1"}, "X:2": {"alias": "DEF:2"}}
    NxGraph.set_node_attributes(g, d)


def test_set_edge_attributes():
    """
    Test setting edge attributes in bulk.
    """
    g = NxGraph()
    g.add_node("X:1", alias="A:1")
    g.add_node("X:2", alias="B:2")
    g.add_edge("X:2", "X:1", edge_key="edge1", source="Source 1")
    d = {("X:2", "X:1", "edge1"): {"source": "Modified Source 1"}}
    NxGraph.set_edge_attributes(g, d)
    e = list(g.edges(keys=True, data=True))[0]
    assert e[3]["source"] == "Modified Source 1"


def test_get_node_attributes():
    """
    Test getting node attributes in bulk.
    """
    g = get_graphs()[1]
    d = NxGraph.get_node_attributes(g, "name")
    assert "A" in d and d["A"] == "Node A"
    assert "E" in d and d["E"] == "Node E"


def test_get_edge_attributes():
    """
    Test getting edge attributes in bulk.
    """
    g = get_graphs()[1]
    d = NxGraph.get_edge_attributes(g, "relation")
    assert ("B", "A", "B-biolink:related_to-A") in d
    assert d[("B", "A", "B-biolink:related_to-A")] == "biolink:related_to"


def test_relabel_nodes():
    """
    Test relabelling of nodes in an NxGraph.
    """
    g = get_graphs()[1]
    m = {"A": "A:1", "E": "E:1"}
    NxGraph.relabel_nodes(g, m)
    assert not g.has_node("A")
    assert g.has_node("A:1")
    assert not g.has_node("E")
    assert g.has_node("E:1")

    assert len(g.in_edges("A:1")) == 3
