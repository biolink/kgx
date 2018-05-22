from kgx import LogicTermTransformer, ObanRdfTransformer
from rdflib import Namespace
from rdflib.namespace import RDF
import rdflib


def test_save():
    """
    save tests
    """
    src_path = "tests/resources/monarch/biogrid_test.ttl"
    tg_path = "target/test_output.sxrp"

    t = ObanRdfTransformer()
    t.parse(src_path, input_format="turtle")

    w = LogicTermTransformer(t.graph)
    w.graph = t.graph
    w.save('target/biogrid.sxpr')

    w1 = LogicTermTransformer(t.graph, 'prolog')
    w1.save('target/biogrid.pl')

