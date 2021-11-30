import gzip
import os

from kgx.sink import JsonlSink
from kgx.transformer import Transformer
from tests import TARGET_DIR
from tests.unit.test_sink import get_graph


def test_write_jsonl1():
    """
    Write a graph as JSON Lines using JsonlSink.
    """
    graph = get_graph()
    filename = os.path.join(TARGET_DIR, "test_graph1")
    t = Transformer()
    s = JsonlSink(t, filename=filename)
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()
    del s
    assert os.path.exists(f"{filename}_nodes.jsonl")
    assert os.path.exists(f"{filename}_edges.jsonl")

    node_lines = open(f"{filename}_nodes.jsonl").readlines()
    edge_lines = open(f"{filename}_edges.jsonl").readlines()
    assert len(node_lines) == 6
    assert len(edge_lines) == 6


def test_write_jsonl2():
    """
    Write a graph as compressed JSON Lines using JsonlSink.
    """
    graph = get_graph()
    filename = os.path.join(TARGET_DIR, "test_graph2")
    t = Transformer()
    s = JsonlSink(t,filename=filename, compression="gz")
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()
    del s
    assert os.path.exists(f"{filename}_nodes.jsonl.gz")
    assert os.path.exists(f"{filename}_edges.jsonl.gz")

    node_lines = gzip.open(f"{filename}_nodes.jsonl.gz", "rb").readlines()
    edge_lines = gzip.open(f"{filename}_edges.jsonl.gz", "rb").readlines()

    assert len(node_lines) == 6
    assert len(edge_lines) == 6
