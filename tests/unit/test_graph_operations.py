import pytest

from kgx.graph.nx_graph import NxGraph
from kgx.graph_operations import (
    remove_singleton_nodes,
    fold_predicate,
    unfold_node_property,
    remap_edge_property,
    remap_node_property,
    remap_node_identifier,
)


def get_graphs1():
    """
    Returns instances of defined graphs.
    """
    g1 = NxGraph()
    g1.add_edge("B", "A", **{"predicate": "biolink:sub_class_of"})
    g1.add_edge("C", "B", **{"predicate": "biolink:sub_class_of"})
    g1.add_edge("D", "C", **{"predicate": "biolink:sub_class_of"})
    g1.add_edge("D", "A", **{"predicate": "biolink:related_to"})
    g1.add_edge("E", "D", **{"predicate": "biolink:sub_class_of"})
    g1.add_edge("F", "D", **{"predicate": "biolink:sub_class_of"})

    g2 = NxGraph()
    g2.name = "Graph 1"
    g2.add_node(
        "HGNC:12345",
        id="HGNC:12345",
        name="Test Gene",
        category=["biolink:NamedThing"],
        alias="NCBIGene:54321",
        same_as="UniProtKB:54321",
    )
    g2.add_node("B", id="B", name="Node B", category=["biolink:NamedThing"], alias="Z")
    g2.add_node("C", id="C", name="Node C", category=["biolink:NamedThing"])
    g2.add_edge(
        "C",
        "B",
        edge_key="C-biolink:subclass_of-B",
        subject="C",
        object="B",
        predicate="biolink:subclass_of",
        relation="rdfs:subClassOf",
        provided_by="Graph 1",
        publications=[1],
        pubs=["PMID:123456"],
    )
    g2.add_edge(
        "B",
        "A",
        edge_key="B-biolink:subclass_of-A",
        subject="B",
        object="A",
        predicate="biolink:subclass_of",
        relation="rdfs:subClassOf",
        provided_by="Graph 1",
    )
    g2.add_edge(
        "C",
        "c",
        edge_key="C-biolink:exact_match-B",
        subject="C",
        object="c",
        predicate="biolink:exact_match",
        relation="skos:exactMatch",
        provided_by="Graph 1",
    )

    return [g1, g2]


def get_graphs2():
    """
    Returns instances of defined graphs.
    """
    g1 = NxGraph()
    g1.name = "Graph 1"
    g1.add_node(
        "HGNC:12345",
        id="HGNC:12345",
        name="Test Gene",
        category=["biolink:NamedThing"],
        alias="NCBIGene:54321",
        same_as="UniProtKB:54321",
    )
    g1.add_node("B", id="B", name="Node B", category=["biolink:NamedThing"], alias="Z")
    g1.add_node("C", id="C", name="Node C", category=["biolink:NamedThing"])
    g1.add_edge(
        "C",
        "B",
        edge_key="C-biolink:subclass_of-B",
        subject="C",
        object="B",
        predicate="biolink:subclass_of",
        relation="rdfs:subClassOf",
        provided_by="Graph 1",
        publications=[1],
        pubs=["PMID:123456"],
    )
    g1.add_edge(
        "B",
        "A",
        edge_key="B-biolink:subclass_of-A",
        subject="B",
        object="A",
        predicate="biolink:subclass_of",
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
        category=["biolink:Gene"],
        xref=["NCBIGene:12345", "HGNC:001033"],
    )
    g2.add_node(
        "B",
        id="B",
        name="Node B",
        description="Node B in Graph 2",
        category=["biolink:Gene"],
        xref=["NCBIGene:56463", "HGNC:012901"],
    )
    g2.add_node(
        "C",
        id="C",
        name="Node C",
        description="Node C in Graph 2",
        category=["biolink:Gene", "biolink:NamedThing"],
        xref=["NCBIGene:08239", "HGNC:103431"],
    )
    g2.add_node(
        "D",
        id="D",
        name="Node D",
        description="Node D in Graph 2",
        category=["biolink:Gene"],
        xref=["HGNC:394233"],
    )
    g2.add_node(
        "E",
        id="E",
        name="Node E",
        description="Node E in Graph 2",
        category=["biolink:NamedThing"],
        xref=["NCBIGene:X", "HGNC:X"],
    )
    g2.add_node(
        "F",
        id="F",
        name="Node F",
        description="Node F in Graph 2",
        category=["biolink:NamedThing"],
        xref=["HGNC:Y"],
    )
    g2.add_edge(
        "B",
        "A",
        edge_key="B-biolink:subclass_of-A",
        subject="B",
        object="A",
        predicate="biolink:subclass_of",
        relation="rdfs:subClassOf",
        provided_by="Graph 2",
    )
    g2.add_edge(
        "B",
        "A",
        edge_key="B-biolink:related_to-A",
        subject="B",
        object="A",
        predicate="biolink:related_to",
        relation="biolink:related_to",
    )
    g2.add_edge(
        "D",
        "A",
        edge_key="D-biolink:related_to-A",
        subject="D",
        object="A",
        predicate="biolink:related_to",
        relation="biolink:related_to",
    )
    g2.add_edge(
        "E",
        "A",
        edge_key="E-biolink:related_to-A",
        subject="E",
        object="A",
        predicate="biolink:related_to",
        relation="biolink:related_to",
    )
    g2.add_edge(
        "E",
        "F",
        edge_key="F-biolink:related_to-A",
        subject="E",
        object="F",
        predicate="biolink:related_to",
        relation="biolink:related_to",
    )

    return [g1, g2]


def test_fold_predicate1():
    """
    Test the fold_predicate operation.
    """
    g = get_graphs1()[1]
    fold_predicate(g, "biolink:exact_match")
    assert not g.has_edge("C", "c")
    n = g.nodes(data=True)["C"]
    assert "biolink:exact_match" in n and n["biolink:exact_match"] == "c"


def test_fold_predicate2():
    """
    Test the fold predicate operation, where the prefix of
    the predicate is removed.
    """
    g = get_graphs1()[1]
    fold_predicate(g, "biolink:exact_match", remove_prefix=True)
    assert not g.has_edge("C", "c")
    n = g.nodes(data=True)["C"]
    assert "exact_match" in n and n["exact_match"] == "c"


def test_unfold_node_property1():
    """Test the unfold node property operation."""
    g = get_graphs1()[1]
    unfold_node_property(g, "same_as")
    assert "same_as" not in g.nodes()["HGNC:12345"]
    assert g.has_edge("HGNC:12345", "UniProtKB:54321")
    e = list(dict(g.get_edge("HGNC:12345", "UniProtKB:54321")).values())[0]
    assert "subject" in e and e["subject"] == "HGNC:12345"
    assert "predicate" in e and e["predicate"] == "same_as"
    assert "object" in e and e["object"] == "UniProtKB:54321"


def test_unfold_node_property2():
    """
    Test the unfold node property operation, where the prefix of
    the predicate is added explicitly.
    """
    g = get_graphs1()[1]
    unfold_node_property(g, "same_as", prefix="biolink")
    assert "same_as" not in g.nodes()["HGNC:12345"]
    assert g.has_edge("HGNC:12345", "UniProtKB:54321")
    e = list(dict(g.get_edge("HGNC:12345", "UniProtKB:54321")).values())[0]
    assert "subject" in e and e["subject"] == "HGNC:12345"
    assert "predicate" in e and e["predicate"] == "biolink:same_as"
    assert "object" in e and e["object"] == "UniProtKB:54321"


def test_remove_singleton_nodes():
    """
    Test the remove singleton nodes operation.
    """
    g = NxGraph()
    g.add_edge("A", "B")
    g.add_edge("B", "C")
    g.add_edge("C", "D")
    g.add_edge("B", "D")
    g.add_node("X")
    g.add_node("Y")
    assert g.number_of_nodes() == 6
    assert g.number_of_edges() == 4
    remove_singleton_nodes(g)
    assert g.number_of_nodes() == 4
    assert g.number_of_edges() == 4


def test_remap_node_identifier_alias():
    """
    Test remap node identifier operation.
    """
    graphs = get_graphs2()
    g = remap_node_identifier(
        graphs[0], "biolink:NamedThing", alternative_property="alias"
    )
    assert g.has_node("NCBIGene:54321")
    assert g.has_node("Z")
    assert g.has_node("C")
    assert g.has_edge("C", "Z")
    assert g.has_edge("Z", "A")
    assert not g.has_edge("C", "B")
    assert not g.has_edge("B", "A")

    e1 = list(g.get_edge("C", "Z").values())[0]
    assert e1["subject"] == "C" and e1["object"] == "Z"
    assert e1["edge_key"] == "C-biolink:subclass_of-Z"

    e2 = list(g.get_edge("Z", "A").values())[0]
    assert e2["subject"] == "Z" and e2["object"] == "A"
    assert e2["edge_key"] == "Z-biolink:subclass_of-A"


def test_remap_node_identifier_xref():
    """
    Test remap node identifier operation.
    """
    graphs = get_graphs2()
    g = remap_node_identifier(
        graphs[1], "biolink:Gene", alternative_property="xref", prefix="NCBIGene"
    )
    assert g.has_node("NCBIGene:12345")
    assert g.has_node("NCBIGene:56463")
    assert g.has_node("NCBIGene:08239")
    assert g.has_node("D")
    assert g.has_node("E")
    assert g.has_node("F")
    assert not g.has_node("A")
    assert not g.has_node("B")
    assert not g.has_node("C")

    e1 = list(g.get_edge("NCBIGene:56463", "NCBIGene:12345").values())[0]
    assert e1["subject"] == "NCBIGene:56463" and e1["object"] == "NCBIGene:12345"

    e2 = list(g.get_edge("D", "NCBIGene:12345").values())[0]
    assert e2["subject"] == "D" and e2["object"] == "NCBIGene:12345"

    e3 = list(g.get_edge("E", "NCBIGene:12345").values())[0]
    assert e3["subject"] == "E" and e3["object"] == "NCBIGene:12345"

    e4 = list(g.get_edge("E", "F").values())[0]
    assert e4["subject"] == "E" and e4["object"] == "F"


def test_remap_node_property():
    """
    Test remap node property operation.
    """
    graphs = get_graphs2()
    remap_node_property(
        graphs[0],
        category="biolink:NamedThing",
        old_property="alias",
        new_property="same_as",
    )
    assert graphs[0].nodes()["HGNC:12345"]["alias"] == "UniProtKB:54321"


def test_remap_node_property_fail():
    """
    Test remap node property operation, where the test fails due to an attempt
    to change a core node property.
    """
    graphs = get_graphs2()
    with pytest.raises(AttributeError):
        remap_node_property(
            graphs[0],
            category="biolink:NamedThing",
            old_property="id",
            new_property="alias",
        )


@pytest.mark.skip()
def test_remap_edge_property():
    """
    Test remap edge property operation.
    """
    graphs = get_graphs2()
    remap_edge_property(
        graphs[0],
        edge_predicate="biolink:subclass_of",
        old_property="publications",
        new_property="pubs",
    )
    e = list(graphs[0].get_edge("C", "B").values())[0]
    assert e["publications"] == ["PMID:123456"]


def test_remap_edge_property_fail():
    """
    Test remap edge property operation, where the test fails due to an attempt
    to change a core edge property.
    """
    graphs = get_graphs2()
    with pytest.raises(AttributeError):
        remap_edge_property(
            graphs[0],
            edge_predicate="biolink:subclass_of",
            old_property="subject",
            new_property="pubs",
        )

    with pytest.raises(AttributeError):
        remap_edge_property(
            graphs[0],
            edge_predicate="biolink:subclass_of",
            old_property="object",
            new_property="pubs",
        )

    with pytest.raises(AttributeError):
        remap_edge_property(
            graphs[0],
            edge_predicate="biolink:subclass_of",
            old_property="predicate",
            new_property="pubs",
        )
