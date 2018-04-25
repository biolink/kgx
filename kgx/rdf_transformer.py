import pandas as pd
import logging
from .transformer import Transformer

import rdflib
from rdflib import Namespace
from rdflib.namespace import RDF
from prefixcommons.curie_util import contract_uri

OBAN = Namespace('http://purl.org/oban/')

mapping = {
    'subject': OBAN.association_has_subject,
    'object': OBAN.association_has_object,
    'predicate': OBAN.association_has_predicate
}
rmapping = {y:x for x,y in mapping.items()}

class RdfTransformer(Transformer):
    """
    Transforms to and from RDF

    We support different RDF metamodels, including:

     - OBAN reification (as used in Monarch)
     - RDF reification

    TODO: we will have some of the same logic if we go from a triplestore. How to share this?
    """

    def parse(self,filename=None, format=None):
        """
        Parse a file into an graph, using rdflib
        """
        rdfgraph = rdflib.Graph()
        if format is None:
            if filename.endswith(".ttl"):
                format='turtle'
            elif filename.endswith(".rdf"):
                format='xml'
        rdfgraph.parse(filename, format=format)

        # TODO: use source from RDF
        self.graph_metadata['provided_by'] = filename
        self.load_edges(rdfgraph)

        
    
class ObanRdfTransformer(RdfTransformer):
    """
    Transforms to and from RDF, assuming OBAN-style modeling
    """
    
    def load_edges(self, rg):
        """
        """
        items = []
        for a in rg.subjects(RDF.type, OBAN.association):
            obj = {}
            for s,p,o in rg.triples( (a, None, None) ):
                if p in rmapping:
                    p = rmapping[p]
                v = self.curie(o)
                obj[p] = v
            s = obj['subject']
            o = obj['object']
            obj['provided_by'] = self.graph_metadata['provided_by']
            self.graph.add_edge(o, s, attr_dict=obj)
            
    def curie(self, uri):
        curies = contract_uri(str(uri))
        if len(curies)>0:
            return curies[0]
        return str(uri)
