import json
import os

from kgx.sink import JsonSink
from kgx.transformer import Transformer
from tests import TARGET_DIR
from tests.unit.test_sink import get_graph


def test_write_json1():
    """
    Write a graph as JSON using JsonSink.
    """
    graph = get_graph()
    filename = os.path.join(TARGET_DIR, "test_graph1.json")
    t = Transformer()
    s = JsonSink(t, filename=filename)
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()
    assert os.path.exists(filename)


def test_write_json2():
    """
    Write a graph as a compressed JSON using JsonSink.
    """
    graph = get_graph()
    filename = os.path.join(TARGET_DIR, "test_graph2.json")
    t = Transformer()
    s = JsonSink(t, filename=filename, compression="gz")
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()
    assert os.path.exists(f"{filename}.gz")
