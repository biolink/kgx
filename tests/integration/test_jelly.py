import os
from kgx.transformer import Transformer
from tests import RESOURCE_DIR, TARGET_DIR


def test_jelly_roundtrip_basic():
    """
    Test basic roundtrip: transform TSV to Jelly format and back.
    """
    input_args = {
        "filename": [
            os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
            os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
        ],
        "format": "tsv",
    }

    t1 = Transformer()
    t1.transform(input_args)

    out_file = os.path.join(TARGET_DIR, "rt_jelly.jelly")
    t1.save({"filename": out_file, "format": "jelly"})

    assert os.path.exists(out_file)
    assert os.path.getsize(out_file) > 0

    t2 = Transformer()
    t2.transform({"filename": [out_file], "format": "jelly"})


def test_jelly_read_only():
    """
    Test reading a Jelly format file.
    """
    jelly_file = os.path.join(TARGET_DIR, "rt_jelly.jelly")

    t = Transformer()
    t.transform({"filename": [jelly_file], "format": "jelly"})


def test_jelly_write_only():
    """
    Test writing a graph to Jelly format.
    """
    input_args = {
        "filename": [
            os.path.join(RESOURCE_DIR, "test2_nodes.tsv"),
            os.path.join(RESOURCE_DIR, "test2_edges.tsv"),
        ],
        "format": "tsv",
    }

    t = Transformer()
    t.transform(input_args)

    out = os.path.join(TARGET_DIR, "write_only.jelly")
    t.save({"filename": out, "format": "jelly"})

    assert os.path.exists(out)
    assert os.path.getsize(out) > 0


def test_jelly_filters():
    """
    Test Jelly format with node filters applied.
    """
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

    assert os.path.exists(out)

    t2 = Transformer()
    t2.transform({"filename": [out], "format": "jelly"})


def test_jelly_to_tsv():
    """
    Test converting a Jelly format file back to TSV format.
    """
    jelly_file = os.path.join(TARGET_DIR, "rt_jelly.jelly")
    out_prefix = os.path.join(TARGET_DIR, "from_jelly")

    t = Transformer()
    t.transform(
        {"filename": [jelly_file], "format": "jelly"},
        {"filename": out_prefix, "format": "tsv"},
    )

    assert os.path.exists(out_prefix + "_nodes.tsv")
    assert os.path.exists(out_prefix + "_edges.tsv")
