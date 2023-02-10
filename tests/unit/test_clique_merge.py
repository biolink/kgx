from kgx.graph.nx_graph import NxGraph
from kgx.graph_operations.clique_merge import (
    check_categories,
    sort_categories,
    check_all_categories,
    clique_merge,
)
from kgx.utils.kgx_utils import get_biolink_ancestors, generate_edge_key, get_toolkit
from tests import print_graph
from bmt import Toolkit


def test_check_categories():
    """
    Test check_categories method.
    """
    vbc, ibc, ic = check_categories(
        ["biolink:Gene"], get_biolink_ancestors("biolink:Gene"), None
    )
    assert "biolink:Gene" in vbc
    assert len(ibc) == 0

    vbc, ibc, ic = check_categories(
        ["biolink:BiologicalEntity"], get_biolink_ancestors("biolink:Disease"), None
    )
    assert "biolink:BiologicalEntity" in vbc
    assert len(ibc) == 0

    vbc, ibc, ic = check_categories(
        ["biolink:Disease"], get_biolink_ancestors("biolink:Gene"), None
    )
    assert len(vbc) == 0
    assert len(ibc) == 1 and "biolink:Disease" in ibc


def test_check_all_categories1():
    """
    Test check_all_categories method.

    ibc = invalid biolink categories
    ic = invalid category
    vbc = valid biolink categories

    Note: in the check_categories method called by check_all_categories, the
    categories list in this test gets sorted to be processed like this:
    ['biolink:Gene', 'biolink:Disease', 'biolink:NamedThing', 'biolink:GeneOrGeneProduct']

    Which effects which closure is checked for valid biolink categories and is why disease is
    tagged as an invalid biolink category (even though it descends from biolink:NamedThing).

    GeneOrGeneProduct is a mixin, and therefore not considered a valid 'category' even though it is
    in the 'biolink:Gene' hierarchy.

    """
    categories = [
        "biolink:Disease",
        "biolink:Gene",
        "biolink:GeneOrGeneProduct",
        "biolink:NamedThing",
    ]
    vbc, ibc, ic = check_all_categories(categories)
    assert len(vbc) == 2
    assert len(ibc) == 1 and "biolink:Disease" in ibc
    assert (
        len(ic) == 1
    )  # since biolink:GeneOrGeneProduct is a mixin, it is declared as an invalid category.


def test_check_all_categories2():
    """
    Test check_all_categories method.
    """
    categories = get_biolink_ancestors("biolink:Gene")
    vbc, ibc, ic = check_all_categories(categories)

    assert len(vbc) == 4
    assert len(ibc) == 0
    assert (
        len(ic) == 8
    )  # mixins are not valid biolink categories, but they are ancestors.

    categories = ["biolink:NamedThing", "biolink:GeneOrGeneProduct", "biolink:Gene"]
    vbc, ibc, ic = check_all_categories(categories)
    assert len(vbc) == 2
    assert len(ibc) == 0
    assert len(ic) == 1

    categories = ["biolink:NamedThing", "biolink:GeneOrGeneProduct", "Node"]
    vbc, ibc, ic = check_all_categories(categories)
    assert len(vbc) == 1
    assert len(ibc) == 0
    assert len(ic) == 2


def test_sort_categories():
    """
    Test sort_categories method.
    """
    categories = ["biolink:NamedThing", "biolink:BiologicalEntity", "biolink:Disease"]
    sorted_categories = sort_categories(categories)
    assert sorted_categories.index("biolink:Disease") == 0
    assert sorted_categories.index("biolink:BiologicalEntity") == 1
    assert sorted_categories.index("biolink:NamedThing") == 2


def test_clique_merge1():
    """
    Test to perform a clique merge where all nodes in a clique are valid.
    """
    ppm = {"biolink:Gene": ["HGNC", "NCBIGene", "ENSEMBL", "OMIM"]}
    g1 = NxGraph()
    g1.add_node("HGNC:1", **{"category": ["biolink:Gene"]})
    g1.add_node("OMIM:2", **{"category": ["biolink:Gene"]})
    g1.add_node("NCBIGene:3", **{"category": ["biolink:Gene"]})
    g1.add_node("ENSEMBL:4", **{"category": ["biolink:Gene"]})

    g1.add_node("ENSEMBL:6", **{"category": ["biolink:Gene"]})
    g1.add_node("HGNC:7", **{"category": ["biolink:Gene"]})
    g1.add_node("NCBIGene:8", **{"category": ["biolink:Gene"]})

    g1.add_edge(
        "ENSEMBL:4",
        "HGNC:1",
        edge_key=generate_edge_key("ENSEMBL:4", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "NCBIGene:3",
        "HGNC:1",
        edge_key=generate_edge_key("NCBIGene:3", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "OMIM:2",
        "HGNC:1",
        edge_key=generate_edge_key("OMIM:2", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    g1.add_edge(
        "ENSEMBL:6",
        "NCBIGene:8",
        edge_key=generate_edge_key("ENSEMBL:6", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "HGNC:7",
        "NCBIGene:8",
        edge_key=generate_edge_key("HGNC:7", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    updated_graph, clique_graph = clique_merge(
        target_graph=g1, prefix_prioritization_map=ppm
    )
    print_graph(updated_graph)
    assert updated_graph.number_of_nodes() == 2
    assert updated_graph.number_of_edges() == 0
    assert updated_graph.has_node("HGNC:1")
    assert updated_graph.has_node("HGNC:7")

    n1 = updated_graph.nodes()["HGNC:1"]
    assert "OMIM:2" in n1["same_as"]
    assert "NCBIGene:3" in n1["same_as"]
    assert "ENSEMBL:4" in n1["same_as"]

    n2 = updated_graph.nodes()["HGNC:7"]
    assert "ENSEMBL:6" in n2["same_as"]
    assert "NCBIGene:8" in n2["same_as"]

    assert not updated_graph.has_node("OMIM:2")
    assert not updated_graph.has_node("NCBIGene:3")
    assert not updated_graph.has_node("ENSEMBL:4")
    assert not updated_graph.has_node("ENSEMBL:6")
    assert not updated_graph.has_node("NCBIGene:8")


def test_clique_merge2():
    """
    Test to perform clique merge where all nodes in a clique are
    valid but one node has a less specific category.
    """
    ppm = {"biolink:Gene": ["HGNC", "NCBIGene", "ENSEMBL", "OMIM"]}
    g1 = NxGraph()
    g1.add_node("HGNC:1", **{"category": ["biolink:Gene"]})
    g1.add_node("OMIM:2", **{"category": ["biolink:NamedThing"]})
    g1.add_node("NCBIGene:3", **{"category": ["biolink:NamedThing"]})
    g1.add_node("ENSEMBL:4", **{"category": ["biolink:Gene"]})

    g1.add_node("ENSEMBL:6", **{"category": ["biolink:Gene"]})
    g1.add_node("HGNC:7", **{"category": ["biolink:NamedThing"]})
    g1.add_node("NCBIGene:8", **{"category": ["biolink:Gene"]})

    g1.add_edge(
        "ENSEMBL:4",
        "HGNC:1",
        edge_key=generate_edge_key("ENSEMBL:4", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "NCBIGene:3",
        "HGNC:1",
        edge_key=generate_edge_key("NCBIGene:3", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "OMIM:2",
        "HGNC:1",
        edge_key=generate_edge_key("OMIM:2", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    g1.add_edge(
        "ENSEMBL:6",
        "NCBIGene:8",
        edge_key=generate_edge_key("ENSEMBL:6", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "HGNC:7",
        "NCBIGene:8",
        edge_key=generate_edge_key("HGNC:7", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    updated_graph, clique_graph = clique_merge(
        target_graph=g1, prefix_prioritization_map=ppm
    )

    assert updated_graph.number_of_nodes() == 2
    assert updated_graph.number_of_edges() == 0
    assert updated_graph.has_node("HGNC:1")
    assert updated_graph.has_node("HGNC:7")

    n1 = updated_graph.nodes()["HGNC:1"]
    assert "OMIM:2" in n1["same_as"]
    assert "NCBIGene:3" in n1["same_as"]
    assert "ENSEMBL:4" in n1["same_as"]

    n2 = updated_graph.nodes()["HGNC:7"]
    assert "ENSEMBL:6" in n2["same_as"]
    assert "NCBIGene:8" in n2["same_as"]

    assert not updated_graph.has_node("OMIM:2")
    assert not updated_graph.has_node("NCBIGene:3")
    assert not updated_graph.has_node("ENSEMBL:4")
    assert not updated_graph.has_node("ENSEMBL:6")
    assert not updated_graph.has_node("NCBIGene:8")


def test_clique_merge3():
    """
    Test for clique merge where each clique has a node that
    has a non-biolink category.
    """
    ppm = {"biolink:Gene": ["HGNC", "NCBIGene", "ENSEMBL", "OMIM"]}
    g1 = NxGraph()
    g1.add_node("HGNC:1", **{"category": ["biolink:Gene"]})
    g1.add_node("OMIM:2", **{"category": ["biolink:NamedThing", "Node"]})
    g1.add_node("NCBIGene:3", **{"category": ["biolink:NamedThing"]})
    g1.add_node("ENSEMBL:4", **{"category": ["biolink:Gene"]})

    g1.add_node("ENSEMBL:6", **{"category": ["biolink:Gene"]})
    g1.add_node("HGNC:7", **{"category": ["biolink:NamedThing"]})
    g1.add_node("NCBIGene:8", **{"category": ["biolink:NamedThing", "Node"]})

    g1.add_edge(
        "ENSEMBL:4",
        "HGNC:1",
        edge_key=generate_edge_key("ENSEMBL:4", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "NCBIGene:3",
        "HGNC:1",
        edge_key=generate_edge_key("NCBIGene:3", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "OMIM:2",
        "HGNC:1",
        edge_key=generate_edge_key("OMIM:2", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    g1.add_edge(
        "ENSEMBL:6",
        "NCBIGene:8",
        edge_key=generate_edge_key("ENSEMBL:6", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "HGNC:7",
        "NCBIGene:8",
        edge_key=generate_edge_key("HGNC:7", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    updated_graph, clique_graph = clique_merge(
        target_graph=g1, prefix_prioritization_map=ppm
    )

    assert updated_graph.number_of_nodes() == 2
    assert updated_graph.number_of_edges() == 0
    assert updated_graph.has_node("HGNC:1")
    assert updated_graph.has_node("HGNC:7")

    n1 = updated_graph.nodes()["HGNC:1"]
    assert "OMIM:2" in n1["same_as"]
    assert "NCBIGene:3" in n1["same_as"]
    assert "ENSEMBL:4" in n1["same_as"]

    n2 = updated_graph.nodes()["HGNC:7"]
    assert "ENSEMBL:6" in n2["same_as"]
    assert "NCBIGene:8" in n2["same_as"]

    assert not updated_graph.has_node("OMIM:2")
    assert not updated_graph.has_node("NCBIGene:3")
    assert not updated_graph.has_node("ENSEMBL:4")
    assert not updated_graph.has_node("ENSEMBL:6")
    assert not updated_graph.has_node("NCBIGene:8")


def test_clique_merge4():
    """
    Test for clique merge where each clique has a node that has
    a disjoint category from other nodes in a clique. (strict)
    """
    ppm = {"biolink:Gene": ["HGNC", "NCBIGene", "ENSEMBL", "OMIM"]}
    g1 = NxGraph()
    g1.add_node("HGNC:1", **{"category": ["biolink:Gene"]})
    g1.add_node("OMIM:2", **{"category": ["biolink:Gene", "biolink:Disease"]})
    g1.add_node("NCBIGene:3", **{"category": ["biolink:NamedThing"]})
    g1.add_node("ENSEMBL:4", **{"category": ["biolink:Gene"]})

    g1.add_node("ENSEMBL:6", **{"category": ["biolink:Gene"]})
    g1.add_node("HGNC:7", **{"category": ["biolink:NamedThing"]})
    g1.add_node("NCBIGene:8", **{"category": ["biolink:Gene", "biolink:Disease"]})

    g1.add_edge(
        "ENSEMBL:4",
        "HGNC:1",
        edge_key=generate_edge_key("ENSEMBL:4", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "NCBIGene:3",
        "HGNC:1",
        edge_key=generate_edge_key("NCBIGene:3", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "OMIM:2",
        "HGNC:1",
        edge_key=generate_edge_key("OMIM:2", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    g1.add_edge(
        "ENSEMBL:6",
        "NCBIGene:8",
        edge_key=generate_edge_key("ENSEMBL:6", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "HGNC:7",
        "NCBIGene:8",
        edge_key=generate_edge_key("HGNC:7", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    updated_graph, clique_graph = clique_merge(
        target_graph=g1, prefix_prioritization_map=ppm
    )
    print("clique graph:")
    print_graph(clique_graph)
    print("updated graph:")
    print_graph(updated_graph)
    assert updated_graph.number_of_nodes() == 5
    assert updated_graph.number_of_edges() == 3
    assert updated_graph.has_node("HGNC:1")
    assert updated_graph.has_node("HGNC:7")

    n1 = updated_graph.nodes()["HGNC:1"]
    assert "OMIM:2" not in n1["same_as"]
    assert "NCBIGene:3" in n1["same_as"]
    assert "ENSEMBL:4" in n1["same_as"]

    n2 = updated_graph.nodes()["HGNC:7"]
    assert len(n2["same_as"]) == 0

    assert updated_graph.has_node("OMIM:2")
    assert not updated_graph.has_node("NCBIGene:3")
    assert not updated_graph.has_node("ENSEMBL:4")
    assert updated_graph.has_node("ENSEMBL:6")
    assert updated_graph.has_node("NCBIGene:8")


def test_clique_merge5():
    """
    Test for clique merge where each clique has a node that has
    a disjoint category from other nodes in a clique. (lenient)
    """
    ppm = {"biolink:Gene": ["HGNC", "NCBIGene", "ENSEMBL", "OMIM"]}
    g1 = NxGraph()
    g1.add_node("HGNC:1", **{"category": ["biolink:Gene"]})
    g1.add_node("OMIM:2", **{"category": ["biolink:Gene", "biolink:Disease"]})
    g1.add_node("NCBIGene:3", **{"category": ["biolink:NamedThing"]})
    g1.add_node("ENSEMBL:4", **{"category": ["biolink:Gene"]})

    g1.add_node("ENSEMBL:6", **{"category": ["biolink:Gene"]})
    g1.add_node("HGNC:7", **{"category": ["biolink:NamedThing"]})
    g1.add_node("NCBIGene:8", **{"category": ["biolink:Gene", "biolink:Disease"]})

    g1.add_edge(
        "ENSEMBL:4",
        "HGNC:1",
        edge_key=generate_edge_key("ENSEMBL:4", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "NCBIGene:3",
        "HGNC:1",
        edge_key=generate_edge_key("NCBIGene:3", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "OMIM:2",
        "HGNC:1",
        edge_key=generate_edge_key("OMIM:2", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    g1.add_edge(
        "ENSEMBL:6",
        "NCBIGene:8",
        edge_key=generate_edge_key("ENSEMBL:6", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "HGNC:7",
        "NCBIGene:8",
        edge_key=generate_edge_key("HGNC:7", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    updated_graph, clique_graph = clique_merge(
        target_graph=g1, prefix_prioritization_map=ppm, strict=False
    )
    assert updated_graph.number_of_nodes() == 2
    assert updated_graph.number_of_edges() == 0
    assert updated_graph.has_node("HGNC:1")
    assert updated_graph.has_node("HGNC:7")

    n1 = updated_graph.nodes()["HGNC:1"]
    assert "OMIM:2" in n1["same_as"]
    assert "NCBIGene:3" in n1["same_as"]
    assert "ENSEMBL:4" in n1["same_as"]

    n2 = updated_graph.nodes()["HGNC:7"]
    assert "ENSEMBL:6" in n2["same_as"]
    assert "NCBIGene:8" in n2["same_as"]

    assert not updated_graph.has_node("OMIM:2")
    assert not updated_graph.has_node("NCBIGene:3")
    assert not updated_graph.has_node("ENSEMBL:4")
    assert not updated_graph.has_node("ENSEMBL:6")
    assert not updated_graph.has_node("NCBIGene:8")


def test_clique_merge6():
    """
    Test for clique merge where each clique has a node that has
    a disjoint category from other nodes in a clique and the node is a
    participant in same_as edges.
    """
    ppm = {"biolink:Gene": ["HGNC", "NCBIGene", "ENSEMBL", "OMIM"]}
    g1 = NxGraph()
    g1.add_node("HGNC:1", **{"category": ["biolink:Gene"]})
    g1.add_node("OMIM:2", **{"category": ["biolink:Disease"]})
    g1.add_node("NCBIGene:3", **{"category": ["biolink:NamedThing"]})
    g1.add_node("ENSEMBL:4", **{"category": ["biolink:Gene"]})

    g1.add_node("ENSEMBL:6", **{"category": ["biolink:Gene"]})
    g1.add_node("HGNC:7", **{"category": ["biolink:NamedThing"]})
    g1.add_node("NCBIGene:8", **{"category": ["biolink:Disease"]})

    g1.add_edge(
        "ENSEMBL:4",
        "HGNC:1",
        edge_key=generate_edge_key("ENSEMBL:4", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "NCBIGene:3",
        "HGNC:1",
        edge_key=generate_edge_key("NCBIGene:3", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "OMIM:2",
        "HGNC:1",
        edge_key=generate_edge_key("OMIM:2", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    g1.add_edge(
        "ENSEMBL:6",
        "NCBIGene:8",
        edge_key=generate_edge_key("ENSEMBL:6", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "HGNC:7",
        "NCBIGene:8",
        edge_key=generate_edge_key("HGNC:7", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    updated_graph, clique_graph = clique_merge(
        target_graph=g1, prefix_prioritization_map=ppm
    )
    assert updated_graph.number_of_nodes() == 5
    assert updated_graph.number_of_edges() == 3
    assert updated_graph.has_node("HGNC:1")
    assert updated_graph.has_node("HGNC:7")

    n1 = updated_graph.nodes()["HGNC:1"]
    assert "NCBIGene:3" in n1["same_as"]
    assert "ENSEMBL:4" in n1["same_as"]
    assert "OMIM:2" not in n1["same_as"]

    n2 = updated_graph.nodes()["HGNC:7"]
    assert "ENSEMBL:6" not in n2["same_as"]
    assert "NCBIGene:8" not in n2["same_as"]

    assert updated_graph.has_node("OMIM:2")
    assert not updated_graph.has_node("NCBIGene:3")
    assert not updated_graph.has_node("ENSEMBL:4")
    assert updated_graph.has_node("ENSEMBL:6")
    assert updated_graph.has_node("NCBIGene:8")


def test_clique_merge7():
    """
    Test for clique merge where each clique has a node that has
    a disjoint category from other nodes in a clique and the node is
    not a participant in same_as edges.
    """
    ppm = {"biolink:Gene": ["HGNC", "NCBIGene", "ENSEMBL", "OMIM"]}
    g1 = NxGraph()
    g1.add_node("HGNC:1", **{"category": ["biolink:Gene"]})
    g1.add_node("OMIM:2", **{"category": ["biolink:Disease"]})
    g1.add_node("NCBIGene:3", **{"category": ["biolink:NamedThing"]})
    g1.add_node("ENSEMBL:4", **{"category": ["biolink:Gene"]})

    g1.add_node("ENSEMBL:6", **{"category": ["biolink:Gene"]})
    g1.add_node("HGNC:7", **{"category": ["biolink:Disease"]})
    g1.add_node("NCBIGene:8", **{"category": ["biolink:Gene"]})

    g1.add_edge(
        "ENSEMBL:4",
        "HGNC:1",
        edge_key=generate_edge_key("ENSEMBL:4", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "NCBIGene:3",
        "HGNC:1",
        edge_key=generate_edge_key("NCBIGene:3", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "OMIM:2",
        "HGNC:1",
        edge_key=generate_edge_key("OMIM:2", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    g1.add_edge(
        "ENSEMBL:6",
        "NCBIGene:8",
        edge_key=generate_edge_key("ENSEMBL:6", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "HGNC:7",
        "NCBIGene:8",
        edge_key=generate_edge_key("HGNC:7", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    updated_graph, clique_graph = clique_merge(
        target_graph=g1, prefix_prioritization_map=ppm
    )
    assert updated_graph.number_of_nodes() == 4
    assert updated_graph.number_of_edges() == 2
    assert updated_graph.has_node("HGNC:1")
    assert updated_graph.has_node("NCBIGene:8")

    n1 = updated_graph.nodes()["HGNC:1"]
    assert "NCBIGene:3" in n1["same_as"]
    assert "ENSEMBL:4" in n1["same_as"]
    assert "OMIM:2" not in n1["same_as"]

    n2 = updated_graph.nodes()["NCBIGene:8"]
    assert "ENSEMBL:6" in n2["same_as"]

    assert updated_graph.has_node("OMIM:2")
    assert not updated_graph.has_node("NCBIGene:3")
    assert not updated_graph.has_node("ENSEMBL:4")
    assert updated_graph.has_node("HGNC:7")


def test_clique_merge8():
    """
    Test for clique merge where same_as appear as both node and edge properties.
    """
    ppm = {"biolink:Gene": ["HGNC", "NCBIGene", "ENSEMBL", "OMIM"]}
    g1 = NxGraph()
    g1.add_node("HGNC:1", **{"category": ["biolink:Gene"]})
    g1.add_node("OMIM:2", **{"category": ["biolink:Gene"], "same_as": ["HGNC:1"]})
    g1.add_node("NCBIGene:3", **{"category": ["biolink:NamedThing"]})
    g1.add_node("ENSEMBL:4", **{"category": ["biolink:Gene"], "same_as": ["HGNC:1"]})

    g1.add_node(
        "ENSEMBL:6", **{"category": ["biolink:Gene"], "same_as": ["NCBIGene:8"]}
    )
    g1.add_node("HGNC:7", **{"category": ["biolink:Gene"]})
    g1.add_node("NCBIGene:8", **{"category": ["biolink:Gene"]})

    g1.add_edge(
        "NCBIGene:3",
        "HGNC:1",
        edge_key=generate_edge_key("NCBIGene:3", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    g1.add_edge(
        "ENSEMBL:6",
        "NCBIGene:8",
        edge_key=generate_edge_key("ENSEMBL:6", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "HGNC:7",
        "NCBIGene:8",
        edge_key=generate_edge_key("HGNC:7", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    updated_graph, clique_graph = clique_merge(
        target_graph=g1, prefix_prioritization_map=ppm
    )
    assert updated_graph.number_of_nodes() == 2
    assert updated_graph.number_of_edges() == 0
    assert updated_graph.has_node("HGNC:1")
    assert updated_graph.has_node("HGNC:7")

    n1 = updated_graph.nodes()["HGNC:1"]
    assert "OMIM:2" in n1["same_as"]
    assert "NCBIGene:3" in n1["same_as"]
    assert "ENSEMBL:4" in n1["same_as"]

    n2 = updated_graph.nodes()["HGNC:7"]
    assert "ENSEMBL:6" in n2["same_as"]
    assert "NCBIGene:8" in n2["same_as"]

    assert not updated_graph.has_node("OMIM:2")
    assert not updated_graph.has_node("NCBIGene:3")
    assert not updated_graph.has_node("ENSEMBL:4")
    assert not updated_graph.has_node("ENSEMBL:6")
    assert not updated_graph.has_node("NCBIGene:8")


def test_clique_merge9():
    """
    Test for clique merge where same_as appear as both node and edge properties,
    but an invalid node also has a same_as property and participates in same_as edge.
    """
    ppm = {"biolink:Gene": ["HGNC", "NCBIGene", "ENSEMBL", "OMIM"]}
    g1 = NxGraph()
    g1.add_node("HGNC:1", **{"category": ["biolink:Gene"]})
    g1.add_node("OMIM:2", **{"category": ["biolink:Disease"], "same_as": ["HGNC:1"]})
    g1.add_node("NCBIGene:3", **{"category": ["biolink:NamedThing"]})
    g1.add_node("ENSEMBL:4", **{"category": ["biolink:Gene"], "same_as": ["HGNC:1"]})

    g1.add_node(
        "ENSEMBL:6", **{"category": ["biolink:Gene"], "same_as": ["NCBIGene:8"]}
    )
    g1.add_node("HGNC:7", **{"category": ["biolink:Gene"]})
    g1.add_node("NCBIGene:8", **{"category": ["biolink:Gene"]})

    g1.add_edge(
        "X:00001",
        "OMIM:2",
        edge_key=generate_edge_key("X:00001", "biolink:same_as", "OMIM:2"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "NCBIGene:3",
        "HGNC:1",
        edge_key=generate_edge_key("NCBIGene:3", "biolink:same_as", "HGNC:1"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    g1.add_edge(
        "ENSEMBL:6",
        "NCBIGene:8",
        edge_key=generate_edge_key("ENSEMBL:6", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )
    g1.add_edge(
        "HGNC:7",
        "NCBIGene:8",
        edge_key=generate_edge_key("HGNC:7", "biolink:same_as", "NCBIGene:8"),
        **{"predicate": "biolink:same_as", "relation": "owl:equivalentClass"}
    )

    updated_graph, clique_graph = clique_merge(
        target_graph=g1, prefix_prioritization_map=ppm
    )
    assert updated_graph.number_of_nodes() == 4
    assert updated_graph.number_of_edges() == 1
    assert updated_graph.has_node("HGNC:1")
    assert updated_graph.has_node("HGNC:7")

    n1 = updated_graph.nodes()["HGNC:1"]
    assert "OMIM:2" not in n1["same_as"]
    assert "NCBIGene:3" in n1["same_as"]
    assert "ENSEMBL:4" in n1["same_as"]

    n2 = updated_graph.nodes()["HGNC:7"]
    assert "ENSEMBL:6" in n2["same_as"]
    assert "NCBIGene:8" in n2["same_as"]

    assert updated_graph.has_node("OMIM:2")
