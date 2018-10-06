from .transformer import Transformer

import rdflib
import logging
import uuid
from rdflib import Namespace, URIRef
from rdflib.namespace import RDF, RDFS, OWL
from typing import NewType

from prefixcommons.curie_util import contract_uri, expand_uri, default_curie_maps

UriString = NewType("UriString", str)

OBAN = Namespace('http://purl.org/oban/')

# TODO: use JSON-LD context
mapping = {
    'subject': OBAN.association_has_subject,
    'object': OBAN.association_has_object,
    'predicate': OBAN.association_has_predicate,
    'type' : RDF.type,
    'comment': RDFS.comment,
    'name': RDFS.label
}
rmapping = {y: x for x, y in mapping.items()}


class RdfTransformer(Transformer):
    """
    Transforms to and from RDF

    We support different RDF metamodels, including:

     - OBAN reification (as used in Monarch)
     - RDF reification

    TODO: we will have some of the same logic if we go from a triplestore. How to share this?
    """

    def parse(self, filename:str=None, input_format:str=None, provided_by:str=None):
        """
        Parse a file into an graph, using rdflib
        """
        rdfgraph = rdflib.Graph()
        if input_format is None:
            if filename.endswith(".ttl"):
                input_format = 'turtle'
            elif filename.endswith(".rdf"):
                input_format='xml'
        rdfgraph.parse(filename, format=input_format)
        logging.info("Parsed : {}".format(filename))

        # TODO: use source from RDF
        if provided_by is not None and isinstance(filename, str):
            provided_by = filename
        self.graph_metadata['provided_by'] = provided_by
        self.load_edges(rdfgraph)
        self.load_nodes(rdfgraph)

    def curie(self, uri: UriString) -> str:
        """
        Translate a URI into a CURIE (prefixed identifier)
        """
        pm = self.prefix_manager
        return pm.contract(str(uri))
        #curies = contract_uri(str(uri))
        #if len(curies)>0:
        #    return curies[0]
        #return str(uri)

    def load_nodes(self, rg: rdflib.Graph):
        G = self.graph
        for n in G.nodes(data=True):
            if 'iri' not in n:
                logging.warning("Expected IRI for {}".format(n))
                continue
            iri = URIRef(n['iri'])
            npmap = {}
            for s,p,o in rg.triples( (iri, None, None) ):
                if isinstance(o, rdflib.term.Literal):
                    if p in rmapping:
                        p = rmapping[p]
                    npmap[p] = str(o)
            G.add_node(n, **npmap)

    def load_edges(self, rg: rdflib.Graph):
        pass

    def add_edge(self, o:str, s:str, attr_dict={}):
        sid = self.curie(s)
        oid = self.curie(o)
        self.graph.add_node(sid, iri=str(s))
        self.graph.add_node(oid, iri=str(o))
        self.graph.add_edge(oid, sid, **attr_dict)

class ObanRdfTransformer(RdfTransformer):
    """
    Transforms to and from RDF, assuming OBAN-style modeling
    """
    rprop_set = set(('subject', 'predicate', 'object', 'provided_by', 'id', str(RDF.type)))
    inv_cmap = {}
    cmap = {}

    def __init__(self, graph=None):
        super().__init__(graph)
        # Generate the map and the inverse map from default curie maps, which will be used later.
        for cmap in default_curie_maps:
            for k, v in cmap.items():
                self.inv_cmap[v] = k
                self.cmap[k] = v

    def load_edges(self, rg: rdflib.Graph):
        pm = self.prefix_manager
        for a in rg.subjects(RDF.type, OBAN.association):
            obj = {}
            # Keep the id of this entity (e.g., <https://monarchinitiative.org/MONARCH_08830...>) as the value of 'id'.
            #obj['id'] = pm.contract(str(a))
            obj['id'] = str(a)

            for s, p, o in rg.triples((a, None, None)):
                if p in rmapping:
                    p = rmapping[p]
                else:
                    px = pm.contract(p)
                    if px is not None:
                        p = px
                v = self.curie(o)
                # Handling multi-value issue, i.e. there can be different v(s) for the same p.
                if p not in obj:
                    obj[p] = []
                obj[p].append(v)

            s = obj['subject']
            o = obj['object']
            obj['provided_by'] = self.graph_metadata['provided_by']
            for each_s in s:
                for each_o in o:
                    self.graph.add_edge(each_o, each_s, **obj)

    def curie(self, uri: UriString) -> str:
        curies = contract_uri(str(uri))
        if len(curies) > 0:
            return curies[0]
        return str(uri)

    # move to superclass?
    def uriref(self, id) -> URIRef:
        if id in mapping:
            uri = mapping[id]
        else:
            uri = self.prefix_manager.expand(id)
        return URIRef(uri)

    def save(self, filename: str = None, output_format: str = None, **kwargs):
        """
        Transform the internal graph into the RDF graphs that follow OBAN-style modeling and dump into the file.
        """
        # Make a new rdflib.Graph() instance to generate RDF triples
        rdfgraph = rdflib.Graph()
        # Register OBAN's url prefix (http://purl.org/oban/) as `OBAN` in the namespace.
        rdfgraph.bind('OBAN', str(OBAN))

        # <http://purl.obolibrary.org/obo/RO_0002558> is currently stored as OBO:RO_0002558 rather than RO:0002558
        # because of the bug in rdflib. See https://github.com/RDFLib/rdflib/issues/632
        rdfgraph.bind('OBO', 'http://purl.obolibrary.org/obo/')

        # Using an iterator of (node, adjacency dict) tuples for all nodes,
        # we iterate every edge (only outgoing adjacencies)
        for n, nbrs in self.graph.adjacency():
            a_object = n
            for nbr, eattr in nbrs.items():
                a_subject = nbr
                for entry, adjitem in eattr.items():
                    pred = "relatedTo"
                    if 'predicate' in adjitem:
                        pred = adjitem['predicate'][0]
                    # assoc_id here is used as subject for each entry, e.g.,
                    # <https://monarchinitiative.org/MONARCH_08830...>
                    if 'id' in adjitem and adjitem['id'] is not None:
                        assoc_id = URIRef(adjitem['id'])
                    else:
                        assoc_id = URIRef('urn:uuid:{}'.format(uuid.uuid4()))
                    self.unpack_adjitem(rdfgraph, assoc_id, adjitem)

                    # The remaining ones are then OBAN's properties and corresponding objects. Store them as triples.
                    rdfgraph.add((assoc_id, mapping['subject'], self.uriref(a_subject)))
                    rdfgraph.add((assoc_id, mapping['predicate'], self.uriref(pred)))
                    rdfgraph.add((assoc_id, mapping['object'], self.uriref(a_object)))

        # For now, assume that the default format is turtle if it is not specified.
        if output_format is None:
            output_format = "turtle"

        # Serialize the graph into the file.
        rdfgraph.serialize(destination=filename, format=output_format)

    def unpack_adjitem(self, rdfgraph, assoc_id, adjitem):
        # Iterate adjacency dict, which contains pairs of properties and objects sharing the same subject.
        for prop_id, prop_values in adjitem.items():
            # See whether the current pair's prop/obj is the OBAN's one.
            if prop_id in self.rprop_set:
                continue

            # If not, see whether its props and objs can be registered as curies in namespaces.
            # Once they are registered, URI/IRIs can be shorten using the curies registered in namespaces.
            # e.g., register http://purl.obolibrary.org/obo/ECO_ with ECO.
            if not isinstance(prop_values,list):
                prop_values = [prop_values]
            for prop_value in prop_values:
                obj_uri = self.uriref(prop_value)

                # Store the pair as a triple.
                rdfgraph.add((assoc_id, self.uriref(prop_id), obj_uri))

    def split_uri(self, prop_uri):
        """
        Utility function that splits into URI/IRI into prefix and value, e.g.,
        http://purl.obolibrary.org/obo/RO_0002558 as http://purl.obolibrary.org/obo/RO_ and 0002558
        """
        prop_splits = prop_uri.split('_')
        if len(prop_splits) > 1:
            return prop_splits[0] + "_", prop_splits[1]
        else:
            prop_splits = prop_uri.split('#')
            if len(prop_splits) > 1:
                return prop_splits[0] + "#", prop_splits[1]
            else:
                slash_index = prop_uri.rfind("/")
                return prop_uri[0:slash_index + 1], prop_uri[slash_index + 1:]
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
                    logging.warning("Do not know how to handle: {}".format(o))
            else:
                # C SubClassOf D (C and D are named classes)
                pred = 'owl:subClassOf'
                parent = o
            obj['predicate'] = pred
            obj['provided_by'] = self.graph_metadata['provided_by']
            self.add_edge(parent, s, attr_dict=obj)
