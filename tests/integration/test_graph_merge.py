import os
from kgx.graph_operations.graph_merge import merge_all_graphs
from kgx.transformer import Transformer
from tests import RESOURCE_DIR


def test_merge():
    """
    Test for merging graphs.
    """
    input_args1 = {
        "filename": [
            os.path.join(RESOURCE_DIR, "merge", "test1_nodes.tsv"),
            os.path.join(RESOURCE_DIR, "merge", "test1_edges.tsv"),
        ],
        "format": "tsv",
    }
    t1 = Transformer()
    t1.transform(input_args1)

    input_args2 = {
        "filename": [
            os.path.join(RESOURCE_DIR, "merge", "test2_nodes.tsv"),
            os.path.join(RESOURCE_DIR, "merge", "test2_edges.tsv"),
        ],
        "format": "tsv",
    }
    t2 = Transformer()
    t2.transform(input_args2)

    merged_graph = merge_all_graphs([t1.store.graph, t2.store.graph], preserve=True)
    assert len(merged_graph.nodes()) == 6
    assert len(merged_graph.edges()) == 8

    x1 = merged_graph.nodes()["x1"]
    assert x1["name"] == "node x1"

    assert isinstance(x1["category"], list)
    assert "a" in x1["p1"]
    assert "1" in x1["p1"]

    x10 = merged_graph.nodes()["x10"]
    assert x10["id"] == "x10"
    assert x10["name"] == "node x10"


def test_merge_no_preserve():
    """
    Test for merging graphs, overwriting conflicting properties.
    """
    input_args1 = {
        "filename": [
            os.path.join(RESOURCE_DIR, "merge", "test1_nodes.tsv"),
            os.path.join(RESOURCE_DIR, "merge", "test1_edges.tsv"),
        ],
        "format": "tsv",
    }
    t1 = Transformer()
    t1.transform(input_args1)

    input_args2 = {
        "filename": [
            os.path.join(RESOURCE_DIR, "merge", "test2_nodes.tsv"),
            os.path.join(RESOURCE_DIR, "merge", "test2_edges.tsv"),
        ],
        "format": "tsv",
    }
    t2 = Transformer()
    t2.transform(input_args2)
    merged_graph = merge_all_graphs([t1.store.graph, t2.store.graph], preserve=False)
    assert len(merged_graph.nodes()) == 6
    assert len(merged_graph.edges()) == 8

    x1 = merged_graph.nodes()["x1"]
    assert x1["name"] == "node x1"

    assert isinstance(x1["category"], list)
    assert list(t1.store.graph.nodes()["x1"]["category"])[0] in x1["category"]
    assert list(t2.store.graph.nodes()["x1"]["category"])[0] in x1["category"]
    assert x1["p1"] == "a"
