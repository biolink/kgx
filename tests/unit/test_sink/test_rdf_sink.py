import os

import pytest
import rdflib
from pprint import pprint
from kgx.sink import RdfSink
from kgx.transformer import Transformer
from tests import TARGET_DIR
from tests.unit.test_sink import get_graph


def test_write_rdf1():
    """
    Write a graph as RDF N-Triples using RdfSink without reifying all edges.
    """
    graph = get_graph()
    filename = os.path.join(TARGET_DIR, "test_graph1.nt")

    t = Transformer()
    s = RdfSink(owner=t, filename=filename, reify_all_edges=False)

    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()

    assert os.path.exists(filename)

    lines = open(filename, "r").readlines()
    assert len(lines) == 18


def test_write_rdf2():
    """
    Write a graph as a compressed RDF N-Triples using RdfSink, without reifying all edges.
    """
    graph = get_graph()
    filename = os.path.join(TARGET_DIR, "test_graph2.nt.gz")

    t = Transformer()
    s = RdfSink(owner=t, filename=filename, compression=True, reify_all_edges=False)

    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()

    assert os.path.exists(filename)

    lines = open(filename, "r").readlines()
    assert len(lines) == 18


def test_write_rdf3():
    """
    Write a graph as RDF N-Triples using RdfSink, where all edges are reified.
    """
    graph = get_graph()
    filename = os.path.join(TARGET_DIR, "test_graph3.nt")

    t = Transformer()
    s = RdfSink(owner=t, filename=filename)

    for n, data in graph.nodes(data=True):
        s.write_node(data)
    for u, v, k, data in graph.edges(data=True, keys=True):
        s.write_edge(data)
    s.finalize()

    assert os.path.exists(filename)

    counter = 0
    lines = open(filename, "r").readlines()
    for line in lines:
        if "<https://w3id.org/biolink/vocab/Association>" in line:
            # the lines collection is the entirety of the RDF, we only want to test that the association
            # type is fully expanded.
            counter = counter+1
    assert counter > 0
    assert len(lines) == 42


@pytest.mark.parametrize(
    "query",
    [
        ("id", "uriorcurie", "MONDO:000001", "URIRef", None),
        (
            "name",
            "xsd:string",
            "Test concept name",
            "Literal",
            rdflib.term.URIRef("http://www.w3.org/2001/XMLSchema#string"),
        ),
        ("predicate", "uriorcurie", "biolink:related_to", "URIRef", None),
        ("relation", "uriorcurie", "RO:000000", "URIRef", None),
        ("custom_property1", "uriorcurie", "X:123", "URIRef", None),
        (
            "custom_property2",
            "xsd:float",
            "480.213",
            "Literal",
            rdflib.term.URIRef("http://www.w3.org/2001/XMLSchema#float"),
        ),
    ],
)
def test_prepare_object(query):
    """
    Test internal _prepare_object method.
    """
    t = Transformer()
    sink = RdfSink(t, os.path.join(TARGET_DIR, "test_graph3.nt"))
    o = sink._prepare_object(query[0], query[1], query[2])
    assert type(o).__name__ == query[3]
    if query[4]:
        assert o.datatype == query[4]


@pytest.mark.parametrize(
    "query",
    [("name", "xsd:string"), ("predicate", "uriorcurie"), ("xyz", "xsd:string")],
)
def test_get_property_type(query):
    """
    Test to ensure that get_property_type returns the appropriate type
    for a given property.
    """
    t = Transformer()
    sink = RdfSink(t, os.path.join(TARGET_DIR, "test_graph3.nt"))
    assert sink._get_property_type(query[0]) == query[1]


@pytest.mark.parametrize(
    "query",
    [
        ("MONDO:000001", "URIRef", "http://purl.obolibrary.org/obo/MONDO_000001"),
        ("urn:uuid:12345", "URIRef", "urn:uuid:12345"),
        (":new_prop", "URIRef", "https://www.example.org/UNKNOWN/new_prop"),
    ],
)
def test_uriref(query):
    """
    Test for uriref method.
    """
    t = Transformer()
    sink = RdfSink(t, os.path.join(TARGET_DIR, "test_graph3.nt"))
    x = sink.uriref(query[0])
    assert type(x).__name__ == query[1]
    assert str(x) == query[2]
