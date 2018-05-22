from kgx import LogicTermTransformer, ObanRdfTransformer, RdfOwlTransformer
from rdflib import Namespace
from rdflib.namespace import RDF
import rdflib
import gzip

def test_save1():
    """
    save tests
    """
    src_path = "tests/resources/monarch/biogrid_test.ttl"

    t = ObanRdfTransformer()
    t.parse(src_path, input_format="turtle")
    save(t.graph, 'biogrid')

def test_save2():
    """
    save tests
    """
    f = gzip.open("tests/resources/mody.ttl.gz", 'rb')
    t = RdfOwlTransformer()
    t.parse(f, input_format='ttl')
    save(t.graph, 'mody')
    
# TODO: make this a proper test with assertions
def save(g, outfile):

    w = LogicTermTransformer(g)
    w.save('target/' + outfile + '.sxpr')

    w1 = LogicTermTransformer(g, 'prolog')
    w1.save("target/" + outfile + ".pl")

