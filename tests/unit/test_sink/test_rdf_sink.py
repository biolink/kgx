import os

from kgx.sink import RdfSink
from tests import TARGET_DIR
from tests.unit.test_sink import get_graph


def test_write_rdf1():
    """
    Write a graph as RDF N-Triples using RdfSink.
    """
    graph = get_graph()
    filename = os.path.join(TARGET_DIR, 'test_graph1.nt')

    s = RdfSink(filename=filename)
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()

    assert os.path.exists(filename)

    lines = open(filename, 'r').readlines()
    assert len(lines) == 18


def test_write_rdf2():
    """
    Write a graph as a compressed RDF N-Triples using RdfSink.
    """
    graph = get_graph()
    filename = os.path.join(TARGET_DIR, 'test_graph2.nt.gz')

    s = RdfSink(filename=filename, compression=True)
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()

    assert os.path.exists(filename)

    lines = open(filename, 'r').readlines()
    assert len(lines) == 18


def test_write_rdf3():
    """
    Write a graph as RDF N-Triples using RdfSink, where all edges are reified.
    """
    graph = get_graph()
    filename = os.path.join(TARGET_DIR, 'test_graph3.nt')

    s = RdfSink(filename=filename, reify_all_edges=True)
    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()

    assert os.path.exists(filename)

    lines = open(filename, 'r').readlines()
    assert len(lines) == 42
