from kgx import LogicTermTransformer, ObanRdfTransformer, RdfOwlTransformer
from rdflib import Namespace
from rdflib.namespace import RDF
import rdflib
import gzip
    
# TODO: make this a proper test with assertions
def save(g, outfile):

    w = LogicTermTransformer(g)
    w.save('target/' + outfile + '.sxpr')

    w1 = LogicTermTransformer(g, 'prolog')
    w1.save("target/" + outfile + ".pl")

