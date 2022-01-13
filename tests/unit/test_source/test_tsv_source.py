import os

from kgx.source import TsvSource
from kgx.transformer import Transformer
from tests import RESOURCE_DIR


def test_read_tsv():
    """
    Read a TSV using TsvSource.
    """
    t = Transformer()
    s = TsvSource(t)

    g = s.parse(filename=os.path.join(RESOURCE_DIR, "test_nodes.tsv"), format="tsv")
    nodes = []
    for rec in g:
        if rec:
            nodes.append(rec)
    assert len(nodes) == 3
    nodes.sort()
    n1 = nodes.pop()[-1]
    assert n1["id"] == "CURIE:456"
    assert n1["name"] == "Disease 456"
    assert "biolink:Disease" in n1["category"]
    assert "biolink:NamedThing" in n1["category"]
    assert n1["description"] == '"Node of type Disease, CURIE:456"'

    g = s.parse(filename=os.path.join(RESOURCE_DIR, "test_edges.tsv"), format="tsv")
    edges = []
    for rec in g:
        if rec:
            edges.append(rec)
    e1 = edges.pop()[-1]
    assert "id" in e1
    assert e1["subject"] == "CURIE:123"
    assert e1["object"] == "CURIE:456"
    assert e1["predicate"] == "biolink:related_to"
    assert e1["relation"] == "biolink:related_to"
    assert "PMID:1" in e1["publications"]


def test_read_csv():
    """
    Read a CSV using TsvSource.
    """
    t = Transformer()
    s = TsvSource(t)

    g = s.parse(filename=os.path.join(RESOURCE_DIR, "test_nodes.csv"), format="csv")
    nodes = []
    for rec in g:
        if rec:
            nodes.append(rec)
    assert len(nodes) == 3
    nodes.sort()
    n1 = nodes.pop()[-1]
    assert n1["id"] == "CURIE:456"
    assert n1["name"] == "Disease 456"
    assert "biolink:Disease" in n1["category"]
    assert "biolink:NamedThing" in n1["category"]
    assert n1["description"] == "Node of type Disease, CURIE:456"

    g = s.parse(filename=os.path.join(RESOURCE_DIR, "test_edges.csv"), format="csv")
    edges = []
    for rec in g:
        if rec:
            print(rec)
            edges.append(rec)
    e1 = edges.pop()[-1]
    assert "id" in e1
    assert e1["subject"] == "CURIE:123"
    assert e1["object"] == "CURIE:456"
    assert e1["predicate"] == "biolink:related_to"
    assert e1["relation"] == "biolink:related_to"
    assert "PMID:1" in e1["publications"]


def test_read_tsv_tar_compressed():
    """
    Read a compressed TSV TAR archive using TsvSource.
    """
    t = Transformer()
    s = TsvSource(t)

    g = s.parse(
        filename=os.path.join(RESOURCE_DIR, "test.tar"), format="tsv", compression="tar"
    )
    nodes = []
    edges = []
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges.append(rec)
            else:
                nodes.append(nodes)
    assert len(nodes) == 3
    assert len(edges) == 1


def test_read_tsv_tar_gz_compressed():
    """
    Read a compressed TSV TAR archive using TsvSource.
    """
    t = Transformer()
    s = TsvSource(t)

    g = s.parse(
        filename=os.path.join(RESOURCE_DIR, "test.tar.gz"),
        format="tsv",
        compression="tar.gz",
    )
    nodes = []
    edges = []
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges.append(rec)
            else:
                nodes.append(nodes)
    assert len(nodes) == 3
    assert len(edges) == 1


def test_read_tsv_tar_gz_compressed_inverted_file_order():
    """
    Read a compressed TSV TAR archive using TsvSource, where source tar archive has edge file first, node second.
    """
    t = Transformer()
    s = TsvSource(t)

    g = s.parse(
        filename=os.path.join(RESOURCE_DIR, "test-inverse.tar.gz"),
        format="tsv",
        compression="tar.gz",
    )
    nodes = []
    edges = []
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges.append(rec)
            else:
                nodes.append(nodes)
    assert len(nodes) == 3
    assert len(edges) == 1


def test_incorrect_nodes():
    """
    Test basic validation of a node, where the node is invalid.
    """
    t = Transformer()
    s = TsvSource(t)
    g = s.parse(filename=os.path.join(RESOURCE_DIR, "incomplete_nodes.tsv"), format="tsv")
    nodes = []
    for rec in g:
        if rec:
            nodes.append(rec)
    t.write_report()
