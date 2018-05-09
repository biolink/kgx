import pandas as pd
import logging
from .transformer import Transformer

import rdflib
from rdflib import Namespace
from rdflib import URIRef
from rdflib.namespace import RDF, RDFS, OWL
from prefixcommons.curie_util import contract_uri
from typing import NewType

UriString = NewType("UriString", str)

OBAN = Namespace('http://purl.org/oban/')

# TODO: use JSON-LD context
mapping = {
    'subject': OBAN.association_has_subject,
    'object': OBAN.association_has_object,
    'predicate': OBAN.association_has_predicate,
    'comment': RDFS.comment,
    'name': RDFS.label
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

    def parse(self, filename:str=None, format:str=None, provided_by:str=None):
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
        logging.info("Parsed : {}".format(filename))

        # TODO: use source from RDF
        if provided_by is not None and isinstance(filename, str):
            provided_by = filename
        self.graph_metadata['provided_by'] = provided_by
        self.load_edges(rdfgraph)
        self.load_nodes(rdfgraph)

    def curie(self, uri: UriString) -> str:
        curies = contract_uri(str(uri))
        if len(curies)>0:
            return curies[0]
        return str(uri)

    def load_nodes(self, rg: rdflib.Graph):
        G = self.graph
        for nid in G.nodes():
            iri = URIRef(G.node[nid]['iri'])
            npmap = {}
            for s,p,o in rg.triples( (iri, None, None) ):
                if isinstance(o, rdflib.term.Literal):
                    if p in rmapping:
                        p = rmapping[p]
                    npmap[p] = str(o)
            G.add_node(nid, **npmap)

    def load_edges(self, rg: rdflib.Graph):
        pass

    def add_edge(self, o:str, s:str, attr_dict={}):
        sid = self.curie(s)
        oid = self.curie(o)
        self.graph.add_node(sid, iri=str(s))
        self.graph.add_node(oid, iri=str(o))
        self.graph.add_edge(oid, sid, attr_dict=attr_dict)

class ObanRdfTransformer(RdfTransformer):
    """
    Transforms to and from RDF, assuming OBAN-style modeling
    """

    def load_edges(self, rg: rdflib.Graph):
        """
        """
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
            self.add_edge(o, s, attr_dict=obj)

class RdfOwlTransformer(RdfTransformer):
    """
    Transforms from an OWL ontology in RDF, retaining class-class
    relationships
    """

    def load_edges(self, rg: rdflib.Graph):
        """
        """
        for s,p,o in rg.triples( (None,RDFS.subClassOf,None) ):
            if isinstance(s, rdflib.term.BNode):
                next
            pred = None
            parent = None
            obj = {}
            if isinstance(o, rdflib.term.BNode):
                # C SubClassOf R some D
                prop = None
                parent = None
                for x in rg.objects( o, OWL.onProperty ):
                    pred = x
                for x in rg.objects( o, OWL.someValuesFrom ):
                    parent = x
                if pred is None or parent is None:
                    logging.warn("Do not know how to handle: {}".format(o))
            else:
                # C SubClassOf D (C and D are named classes)
                pred = 'owl:subClassOf'
                parent = o
            obj['predicate'] = pred
            obj['provided_by'] = self.graph_metadata['provided_by']
            self.add_edge(parent, s, attr_dict=obj)

    
