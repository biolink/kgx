import os

import networkx as nx
from kgx.graph.nx_graph import NxGraph
from kgx.graph_operations.clique_merge import clique_merge
from kgx.transformer import Transformer
from tests import TARGET_DIR, RESOURCE_DIR

prefix_prioritization_map = {"biolink:Gene": ["HGNC", "NCBIGene", "ENSEMBL", "OMIM"]}


def test_clique_generation():
    """
    Test for generation of cliques.
    """
    input_args = {
        "filename": [
            os.path.join(RESOURCE_DIR, "cm_nodes.csv"),
            os.path.join(RESOURCE_DIR, "cm_edges.csv"),
        ],
        "format": "csv",
    }
    t = Transformer()
    t.transform(input_args)
    updated_graph, clique_graph = clique_merge(
        target_graph=t.store.graph, prefix_prioritization_map=prefix_prioritization_map
    )
    cliques = list(nx.strongly_connected_components(clique_graph))
    assert len(cliques) == 2


def test_clique_merge():
    """
    Test for clique merge.
    """
    input_args = {
        "filename": [
            os.path.join(RESOURCE_DIR, "cm_nodes.csv"),
            os.path.join(RESOURCE_DIR, "cm_edges.csv"),
        ],
        "format": "csv",
    }
    t = Transformer()
    t.transform(input_args)
    updated_graph, clique_graph = clique_merge(
        target_graph=t.store.graph, prefix_prioritization_map=prefix_prioritization_map
    )
    leaders = NxGraph.get_node_attributes(updated_graph, "clique_leader")
    leader_list = list(leaders.keys())
    leader_list.sort()
    assert len(leader_list) == 2
    n1 = updated_graph.nodes()[leader_list[0]]
    assert n1["election_strategy"] == "PREFIX_PRIORITIZATION"
    assert "NCBIGene:100302240" in n1["same_as"]
    assert "ENSEMBL:ENSG00000284458" in n1["same_as"]
    n2 = updated_graph.nodes()[leader_list[1]]
    assert n2["election_strategy"] == "PREFIX_PRIORITIZATION"
    assert "NCBIGene:8202" in n2["same_as"]
    assert "OMIM:601937" in n2["same_as"]
    assert "ENSEMBL:ENSG00000124151" not in n2["same_as"]


def test_clique_merge_edge_consolidation():
    """
    Test for clique merge, with edge consolidation.
    """
    input_args = {
        "filename": [
            os.path.join(RESOURCE_DIR, "cm_test2_nodes.tsv"),
            os.path.join(RESOURCE_DIR, "cm_test2_edges.tsv"),
        ],
        "format": "tsv",
    }
    t = Transformer()
    t.transform(input_args)
    updated_graph, clique_graph = clique_merge(
        target_graph=t.store.graph, prefix_prioritization_map=prefix_prioritization_map
    )
    leaders = NxGraph.get_node_attributes(updated_graph, "clique_leader")
    leader_list = list(leaders.keys())
    leader_list.sort()
    assert len(leader_list) == 2

    n1 = updated_graph.nodes()[leader_list[0]]
    assert n1["election_strategy"] == "LEADER_ANNOTATION"
    assert "NCBIGene:100302240" in n1["same_as"]
    assert "ENSEMBL:ENSG00000284458" in n1["same_as"]

    n2 = updated_graph.nodes()[leader_list[1]]
    assert n2["election_strategy"] == "LEADER_ANNOTATION"
    assert "NCBIGene:8202" in n2["same_as"]
    assert "OMIM:601937" in n2["same_as"]
    assert "ENSEMBL:ENSG00000124151" not in n2["same_as"]

    e1_incoming = updated_graph.in_edges("HGNC:7670", data=True)
    assert len(e1_incoming) == 3

    e1_outgoing = updated_graph.out_edges("HGNC:7670", data=True)
    assert len(e1_outgoing) == 6
