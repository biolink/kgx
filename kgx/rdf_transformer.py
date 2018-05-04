from .transformer import Transformer

import rdflib

from rdflib import Namespace, URIRef
from rdflib.namespace import RDF
from prefixcommons.curie_util import contract_uri, expand_uri, default_curie_maps
from typing import NewType

UriString = NewType("UriString", str)

OBAN = Namespace('http://purl.org/oban/')

mapping = {
    'subject': OBAN.association_has_subject,
    'object': OBAN.association_has_object,
    'predicate': OBAN.association_has_predicate
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

    def parse(self, filename: str = None, input_format: str = None):
        """
        Parse a file into an graph, using rdflib
        """
        rdfgraph = rdflib.Graph()
        if input_format is None:
            if filename.endswith(".ttl"):
                input_format = 'turtle'
            elif filename.endswith(".rdf"):
                input_format = 'xml'
        rdfgraph.parse(filename, format=input_format)

        # TODO: use source from RDF
        self.graph_metadata['provided_by'] = filename
        self.load_edges(rdfgraph)


class ObanRdfTransformer(RdfTransformer):
    """
    Transforms to and from RDF, assuming OBAN-style modeling
    """
    rprop_set = set(('subject', 'predicate', 'object', 'provided_by', 'id', str(RDF.type)))
    inv_cmap = {}
    cmap = {}

    def __init__(self):
        super().__init__()
        # Generate the map and the inverse map from default curie maps, which will be used later.
        for cmap in default_curie_maps:
            for k, v in cmap.items():
                self.inv_cmap[v] = k
                self.cmap[k] = v

    def load_edges(self, rg: rdflib.Graph):
        for a in rg.subjects(RDF.type, OBAN.association):
            obj = {}
            # Keep the id of this entity (e.g., <https://monarchinitiative.org/MONARCH_08830...>) as the value of 'id'.
            obj['id'] = str(a)

            for s, p, o in rg.triples((a, None, None)):
                if p in rmapping:
                    p = rmapping[p]
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
                    self.graph.add_edge(each_o, each_s, attr_dict=obj)

    def curie(self, uri: UriString) -> str:
        curies = contract_uri(str(uri))
        if len(curies) > 0:
            return curies[0]
        return str(uri)

    def save(self, filename: str = None, output_format: str = None):
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
        for n, nbrs in self.graph.adjacency_iter():
            for nbr, eattr in nbrs.items():
                for entry, adjitem in eattr.items():
                    # entity_id here is used as subject for each entry, e.g.,
                    # <https://monarchinitiative.org/MONARCH_08830...>
                    entity_id = URIRef(adjitem['id'])
                    self.unpack_adjitem(rdfgraph, entity_id, adjitem)

                    # The remaining ones are then OBAN's properties and corresponding objects. Store them as triples.
                    rdfgraph.add((entity_id, mapping['subject'], URIRef(expand_uri(adjitem['subject'][0]))))
                    rdfgraph.add((entity_id, mapping['predicate'], URIRef(expand_uri(adjitem['predicate'][0]))))
                    rdfgraph.add((entity_id, mapping['object'], URIRef(expand_uri(adjitem['object'][0]))))

        # For now, assume that the default format is turtle if it is not specified.
        if output_format is None:
            output_format = "turtle"

        # Serialize the graph into the file.
        rdfgraph.serialize(destination=filename, format=output_format)

    def unpack_adjitem(self, rdfgraph, entity_id, adjitem):
        # Iterate adjacency dict, which contains pairs of properties and objects sharing the same subject.
        for prop_uri, obj_curies in adjitem.items():
            # See whether the current pair's prop/obj is the OBAN's one.
            if prop_uri in self.rprop_set:
                continue

            # If not, see whether its props and objs can be registered as curies in namespaces.
            # Once they are registered, URI/IRIs can be shorten using the curies registered in namespaces.
            # e.g., register http://purl.obolibrary.org/obo/ECO_ with ECO.
            for obj_curie in obj_curies:
                obj_uri = expand_uri(obj_curie)

                obj_compact_prefix, obj_value = obj_curie.split(":")
                obj_long_prefix = self.cmap.get(obj_compact_prefix, None)
                if obj_long_prefix is not None:
                    rdfgraph.bind(obj_compact_prefix, Namespace(obj_long_prefix))

                prop_long_prefix, prop_value = self.split_uri(prop_uri)
                prop_compact_prefix = self.inv_cmap.get(prop_long_prefix, None)
                if prop_compact_prefix is not None:
                    rdfgraph.bind(prop_compact_prefix, Namespace(prop_long_prefix))

                # Store the pair as a triple.
                rdfgraph.add((entity_id, URIRef(prop_uri), URIRef(obj_uri)))

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