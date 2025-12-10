import os
from kgx.transformer import Transformer
from tests import RESOURCE_DIR, TARGET_DIR

def test_jelly_roundtrip_basic():
    input_args = {
        "filename": [
            os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
            os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
        ],
        "format": "tsv",
    }

    t1 = Transformer()
    t1.transform(input_args)

    out_file = os.path.join(TARGET_DIR, "rt_jelly")
    t1.save({"filename": out_file + ".jelly", "format": "jelly"})

    t2 = Transformer()
    t2.transform({"filename": [out_file + ".jelly"], "format": "jelly"})

    assert t2.store.graph.number_of_nodes() == t1.store.graph.number_of_nodes()
    assert t2.store.graph.number_of_edges() == t1.store.graph.number_of_edges()

def test_jelly_read_only():
    filename = os.path.join(TARGET_DIR, "rt_jelly.jelly")

    t = Transformer()
    t.transform({"filename": [filename], "format": "jelly"})

    assert t.store.graph.number_of_nodes() > 0
    assert t.store.graph.number_of_edges() > 0

def test_jelly_write_only():
    input_args = {
        "filename": [
            os.path.join(RESOURCE_DIR, "test2_nodes.tsv"),
            os.path.join(RESOURCE_DIR, "test2_edges.tsv"),
        ],
        "format": "tsv",
    }

    t = Transformer()
    t.transform(input_args)

    out = os.path.join(TARGET_DIR, "write_only")
    t.save({"filename": out + ".jelly", "format": "jelly"})

    assert os.path.exists(out + ".jelly")

def test_jelly_filters():
    input_args = {
        "filename": [
            os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
            os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
        ],
        "format": "tsv",
        "node_filters": {"category": {"biolink:Gene"}},
    }

    t1 = Transformer()
    t1.transform(input_args)

    out = os.path.join(TARGET_DIR, "filters_jelly.jelly")
    t1.save({"filename": out, "format": "jelly"})

    t2 = Transformer()
    t2.transform({"filename": [out], "format": "jelly"})

    assert t2.store.graph.number_of_nodes() == t1.store.graph.number_of_nodes()
    assert t2.store.graph.number_of_edges() == t1.store.graph.number_of_edges()

def test_jelly_to_tsv():
    jelly_file = os.path.join(TARGET_DIR, "rt_jelly.jelly")

    out_prefix = os.path.join(TARGET_DIR, "from_jelly")

    t = Transformer()
    t.transform(
        {"filename": [jelly_file], "format": "jelly"},
        {"filename": out_prefix, "format": "tsv"}
    )

    assert os.path.exists(out_prefix + "_nodes.tsv")
    assert os.path.exists(out_prefix + "_edges.tsv")
