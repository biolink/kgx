import pytest

from kgx.graph.nx_graph import NxGraph
from kgx.utils.graph_utils import get_parents, get_ancestors, curie_lookup


def get_graphs():
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


def test_get_parents():
    """
    Test get_parents method where the parent is fetched
    by walking the graph across a given edge predicate.
    """
    query = ("E", ["D"])
    graph = get_graphs()[0]
    parents = get_parents(graph, query[0], relations=["biolink:sub_class_of"])
    assert len(parents) == len(query[1])
    assert parents == query[1]


def test_get_ancestors():
    """
    Test get_ancestors method where the ancestors are fetched
    by walking the graph across a given edge predicate.
    """
    query = ("E", ["D", "C", "B", "A"])
    graph = get_graphs()[0]
    parents = get_ancestors(graph, query[0], relations=["biolink:sub_class_of"])
    assert len(parents) == len(query[1])
    assert parents == query[1]


@pytest.mark.skip(reason="To be implemented")
def test_get_category_via_superclass():
    """"""
    pass


@pytest.mark.parametrize(
    "query",
    [
        ("rdfs:subClassOf", "sub_class_of"),
        ("owl:equivalentClass", "equivalent_class"),
        ("RO:0000091", "has_disposition"),
    ],
)
def test_curie_lookup(query):
    """
    Test look up of a CURIE.
    """
    s = curie_lookup(query[0])
    assert s == query[1]
