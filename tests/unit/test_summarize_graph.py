import os
import pytest
from deprecation import deprecated

from kgx.graph.nx_graph import NxGraph
from kgx.graph_operations.summarize_graph import (
    summarize_graph,
    generate_graph_stats,
    GraphSummary,
    TOTAL_NODES,
    NODE_CATEGORIES,
    NODE_ID_PREFIXES,
    NODE_ID_PREFIXES_BY_CATEGORY,
    COUNT_BY_CATEGORY,
    COUNT_BY_ID_PREFIXES,
    COUNT_BY_ID_PREFIXES_BY_CATEGORY,
    TOTAL_EDGES,
    EDGE_PREDICATES,
    COUNT_BY_EDGE_PREDICATES,
    COUNT_BY_SPO,
)
from kgx.transformer import Transformer
from tests import RESOURCE_DIR, TARGET_DIR

try:
    from yaml import load, CLoader as Loader
except ImportError:
    from yaml import load, Loader


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
        predicate="biolink:sub_class_of",
        relation="rdfs:subClassOf",
    )
    g1.add_edge(
        "B",
        "A",
        edge_key="B-biolink:subclass_of-A",
        predicate="biolink:sub_class_of",
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
        predicate="biolink:related_to",
        relation="biolink:related_to",
    )
    g2.add_edge(
        "D",
        "A",
        edge_key="D-biolink:related_to-A",
        predicate="biolink:related_to",
        relation="biolink:related_to",
    )
    g2.add_edge(
        "E",
        "A",
        edge_key="E-biolink:related_to-A",
        predicate="biolink:related_to",
        relation="biolink:related_to",
    )

    g3 = NxGraph()
    g3.name = "Graph 3"
    g3.add_edge(
        "F",
        "E",
        edge_key="F-biolink:same_as-E",
        predicate="biolink:same_as",
        relation="OWL:same_as",
    )

    return [g1, g2, g3]


@deprecated(deprecated_in="1.5.8", details="Default is the use streaming graph_summary with inspector")
def test_generate_graph_stats():
    """
    Test for generating graph stats.
    """
    graphs = get_graphs()
    for g in graphs:
        filename = os.path.join(TARGET_DIR, f"{g.name}_stats.yaml")
        generate_graph_stats(g, g.name, filename)
        assert os.path.exists(filename)


@pytest.mark.parametrize(
    "query",
    [
        (
            get_graphs()[0],
            {
                "node_stats": {
                    "total_nodes": 3,
                    "node_categories": ["biolink:NamedThing"],
                    "count_by_category": {
                        "unknown": {"count": 0},
                        "biolink:NamedThing": {"count": 3},
                    },
                },
                "edge_stats": {"total_edges": 2},
                "predicates": ["biolink:subclass_of"],
                "count_by_predicates": {
                    "unknown": {"count": 0},
                    "biolink:subclass_of": {"count": 2},
                },
                "count_by_spo": {
                    "biolink:NamedThing-biolink:subclass_of-biolink:NamedThing": {
                        "count": 2
                    }
                },
            },
        ),
        (
            get_graphs()[1],
            {
                "node_stats": {
                    "total_nodes": 5,
                    "node_categories": ["biolink:NamedThing"],
                    "count_by_category": {
                        "unknown": {"count": 0},
                        "biolink:NamedThing": {"count": 5},
                    },
                },
                "edge_stats": {
                    "total_edges": 3,
                    "predicates": ["biolink:related_to"],
                    "count_by_predicates": {
                        "unknown": {"count": 0},
                        "biolink:related_to": {"count": 3},
                    },
                    "count_by_spo": {
                        "biolink:NamedThing-biolink:related_to-biolink:NamedThing": {
                            "count": 3
                        }
                    },
                },
            },
        ),
        (
            get_graphs()[2],
            {
                "node_stats": {
                    "total_nodes": 2,
                    "node_categories": [],
                    "count_by_category": {"unknown": {"count": 2}},
                },
                "edge_stats": {
                    "total_edges": 1,
                    "predicates": ["biolink:same_as"],
                    "count_by_predicates": {
                        "unknown": {"count": 0},
                        "biolink:same_as": {"count": 1},
                    },
                    "count_by_spo": {"unknown-biolink:same_as-unknown": {"count": 1}},
                },
            },
        ),
    ],
)
def test_summarize_graph(query):
    """
    Test for generating graph stats, and comparing the resulting stats.
    """
    stats = summarize_graph(query[0])
    for k, v in query[1]["node_stats"].items():
        assert v == stats["node_stats"][k]
    for k, v in query[1]["edge_stats"].items():
        assert v == stats["edge_stats"][k]


def test_summarize_graph_inspector():
    """
    Test generate the graph summary by streaming
    graph data through a graph Transformer.process() Inspector
    """
    input_args = {
        "filename": [
            os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
            os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
        ],
        "format": "tsv",
    }

    transformer = Transformer(stream=True)

    inspector = GraphSummary("Test Graph Summary - Streamed")

    # We configure the Transformer with a data flow inspector
    # (Deployed in the internal Transformer.process() call)
    transformer.transform(input_args=input_args, inspector=inspector)

    output_filename = os.path.join(
        TARGET_DIR, "test_graph-summary-from-inspection.yaml"
    )

    # Dump a report to stderr ... will be a JSON document now
    if len(inspector.get_errors()) > 0:
        assert len(inspector.get_errors("Error")) == 0
        assert len(inspector.get_errors("Warning")) > 0
        inspector.write_report(None, "Warning")
    
    with open(output_filename, "w") as gsh:
        inspector.save(gsh)

    with open(output_filename, "r") as gsh:
        data = load(stream=gsh, Loader=Loader)

    assert data["graph_name"] == "Test Graph Summary - Streamed"
    node_stats = data["node_stats"]
    assert node_stats

    assert TOTAL_NODES in node_stats
    assert node_stats[TOTAL_NODES] == 512

    assert NODE_CATEGORIES in node_stats
    node_categories = node_stats[NODE_CATEGORIES]
    assert "biolink:Pathway" in node_categories

    assert NODE_ID_PREFIXES in node_stats
    node_id_prefixes = node_stats[NODE_ID_PREFIXES]
    assert "HGNC" in node_id_prefixes

    assert NODE_ID_PREFIXES_BY_CATEGORY in node_stats
    id_prefixes_by_category = node_stats[NODE_ID_PREFIXES_BY_CATEGORY]

    assert "biolink:Gene" in id_prefixes_by_category
    assert "ENSEMBL" in id_prefixes_by_category["biolink:Gene"]

    assert "biolink:Disease" in id_prefixes_by_category
    assert "MONDO" in id_prefixes_by_category["biolink:Disease"]

    assert "biolink:PhenotypicFeature" in id_prefixes_by_category
    assert "HP" in id_prefixes_by_category["biolink:PhenotypicFeature"]

    assert COUNT_BY_CATEGORY in node_stats
    count_by_category = node_stats[COUNT_BY_CATEGORY]
    assert "biolink:AnatomicalEntity" in count_by_category
    assert count_by_category["biolink:AnatomicalEntity"]["count"] == 20

    assert COUNT_BY_ID_PREFIXES in node_stats
    count_by_id_prefixes = node_stats[COUNT_BY_ID_PREFIXES]
    assert "HP" in count_by_id_prefixes
    assert count_by_id_prefixes["HP"] == 111

    assert COUNT_BY_ID_PREFIXES_BY_CATEGORY in node_stats
    count_by_id_prefixes_by_category = node_stats[COUNT_BY_ID_PREFIXES_BY_CATEGORY]
    assert "biolink:BiologicalProcess" in count_by_id_prefixes_by_category
    biological_process_id_prefix_count = count_by_id_prefixes_by_category[
        "biolink:BiologicalProcess"
    ]
    assert "GO" in biological_process_id_prefix_count
    assert biological_process_id_prefix_count["GO"] == 143

    edge_stats = data["edge_stats"]
    assert edge_stats
    assert TOTAL_EDGES in edge_stats
    assert edge_stats[TOTAL_EDGES] == 539

    assert EDGE_PREDICATES in edge_stats
    assert len(edge_stats[EDGE_PREDICATES]) == 8
    assert "biolink:actively_involved_in" in edge_stats[EDGE_PREDICATES]

    assert COUNT_BY_EDGE_PREDICATES in edge_stats
    assert len(edge_stats[COUNT_BY_EDGE_PREDICATES]) == 9
    assert "biolink:has_phenotype" in edge_stats[COUNT_BY_EDGE_PREDICATES]
    assert edge_stats[COUNT_BY_EDGE_PREDICATES]["biolink:has_phenotype"]["count"] == 124

    assert COUNT_BY_SPO in edge_stats
    assert len(edge_stats[COUNT_BY_SPO]) == 13
    assert "biolink:Gene-biolink:related_to-biolink:Pathway" in edge_stats[COUNT_BY_SPO]
    assert (
        "count"
        in edge_stats[COUNT_BY_SPO]["biolink:Gene-biolink:related_to-biolink:Pathway"]
    )
    assert (
        edge_stats[COUNT_BY_SPO]["biolink:Gene-biolink:related_to-biolink:Pathway"][
            "count"
        ]
        == 16
    )
