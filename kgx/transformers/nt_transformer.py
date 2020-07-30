import gzip
import itertools
import logging
from typing import Set, Optional, Dict

from rdflib import RDF
from rdflib.plugins.parsers.ntriples import NTriplesParser
from rdflib.plugins.serializers.nt import NT11Serializer
from rdflib.term import URIRef, Literal
import networkx as nx

from kgx import RdfTransformer
from kgx.prefix_manager import PrefixManager
from kgx.utils.kgx_utils import get_toolkit, current_time_in_millis, get_biolink_node_properties, \
    get_biolink_property_types, get_biolink_association_types
from kgx.utils.rdf_utils import generate_uuid

FORMATS = ['nt', 'nt.gz']


class NtTransformer(RdfTransformer):
    """
    Transformer that parses n-triples (NT) and loads triples, as nodes and edges, into a networkx.MultiDiGraph

    .. note::
        This is a specialized version of RdfTransformer that doesn't rely on rdflib.Graph when parsing NTs.
        Depending on performance, this implementation will be subsumed into RdfTransformer.

    """

    def __init__(self, source_graph: nx.MultiDiGraph = None, curie_map: Dict = None):
        super().__init__(source_graph, curie_map)
        self.toolkit = get_toolkit()
        self.node_properties = set([URIRef(self.prefix_manager.expand(x)) for x in get_biolink_node_properties()])
        additional_predicates = ['biolink:same_as', 'OBAN:association_has_object', 'OBAN:association_has_subject',
             'OBAN:association_has_predicate', 'OBAN:association_has_object']
        self.node_properties.update([URIRef(self.prefix_manager.expand(x)) for x in additional_predicates])
        self.reified_nodes = set()
        self.count = 0
        self.start = 0
        self.cache = {}
        self.property_types = get_biolink_property_types()

    def set_property_types(self, m):
        for k, v in m.items():
            self.property_types[k] = v

    def parse(self, filename: str = None, input_format: str = None, provided_by: str = None, node_property_predicates: Set[str] = None) -> None:
        """
        Parse a n-triple file into networkx.MultiDiGraph

        The file must be a *.nt formatted file.

        Parameters
        ----------
        filename : str
            File to read from.
        input_format : str
            The input file format. Must be one of ``['nt', 'nt.gz']``
        provided_by : str
            Define the source providing the input file.
        node_property_predicates: Set[str]
            A set of rdflib.URIRef representing predicates that are to be treated as node properties

        """
        p = NTriplesParser(self)
        if node_property_predicates:
            self.node_properties.update([URIRef(self.prefix_manager.expand(x)) for x in node_property_predicates])
        self.start = current_time_in_millis()
        if input_format == FORMATS[0]:
            p.parse(open(filename, 'rb'))
        elif input_format == FORMATS[1]:
            p.parse(gzip.open(filename, 'rb'))
        else:
            raise NameError(f"input_format: {input_format} not supported. Must be one of {FORMATS}")
        logging.info(f"Done parsing {filename}")
        self.dereify(self.reified_nodes)

    def triple(self, s: URIRef, p: URIRef, o: URIRef) -> None:
        """
        Hook for rdflib.plugins.parsers.ntriples.NTriplesParser

        This method will be called by NTriplesParser when reading from a file.

        Parameters
        ----------
        s: rdflib.term.URIRef
            The subject of a triple.
        p: rdflib.term.URIRef
            The predicate of a triple.
        o: rdflib.term.URIRef
            The object of a triple.

        """
        super().triple(s, p, o)

    def save(self, filename: str = None, output_format: str = "nt", reify_all_edges = False, **kwargs) -> None:
        """
        Export networkx.MultiDiGraph into n-triple format.

        Uses rdflib.plugins.serializers.nt.NT11Serializer.

        Parameters
        ----------
        filename: str
            Filename to write to
        output_format: str
            The output format. Must be one of ``['nt', 'nt.gz']``
        kwargs: dict
            Any additional arguments

        """
        nodes_generator = self.export_nodes()
        edges_generator = self.export_edges(reify_all_edges)
        generator = itertools.chain(nodes_generator, edges_generator)
        serializer = NT11Serializer(generator)
        if output_format == FORMATS[0]:
            f = open(filename, 'wb')
        elif output_format == FORMATS[1]:
            f = gzip.open(filename, 'wb')
        else:
            raise NameError(f"output_format: {output_format} not supported. Must be one of {FORMATS}")
        serializer.serialize(f)

    def export_nodes(self):
        for n, data in self.graph.nodes(data=True):
            s = self.uriref(n)
            for k, v in data.items():
                if k in {'id', 'iri'}:
                    continue
                prop_type = self._get_property_type(k)
                p = self.uriref(k)
                logging.info(f"[n] Treating {p} as {prop_type}")
                if isinstance(v, list):
                    for x in v:
                        o = self._prepare_object(k, prop_type, x)
                        yield (s, p, o)
                else:
                    o = self._prepare_object(k, prop_type, v)
                    yield (s, p, o)

    def export_edges(self, reify_all_edges = False):
        cache = []
        associations = get_biolink_association_types()
        print(associations)
        for u, v, k, data in self.graph.edges(data=True, keys=True):
            if reify_all_edges:
                reified_node = self.reify(u, v, k, data)
                s = reified_node['subject']
                p = reified_node['edge_label']
                o = reified_node['object']
                cache.append((s, p, o))
                n = reified_node['id']
                for prop, value in reified_node.items():
                    if prop in {'id', 'association_id', 'edge_key'}:
                        continue
                    prop_type = self._get_property_type(prop)
                    p = self.uriref(prop)
                    if isinstance(value, list):
                        for x in value:
                            o = self._prepare_object(prop, prop_type, x)
                            yield (n, p, o)
                    else:
                        o = self._prepare_object(prop, prop_type, value)
                        yield (n, p, o)
            else:
                if 'type' in data and data['type'] in associations:
                    reified_node = self.reify(u, v, k, data)
                    s = reified_node['subject']
                    p = reified_node['edge_label']
                    o = reified_node['object']
                    cache.append((s, p, o))
                    n = reified_node['id']
                    for prop, value in reified_node.items():
                        if prop in {'id', 'association_id', 'edge_key'}:
                            continue
                        prop_type = self._get_property_type(prop)
                        p = self.uriref(prop)
                        logging.info(f"[e] Treating {p} as {prop_type}")
                        if isinstance(value, list):
                            for x in value:
                                o = self._prepare_object(prop, prop_type, x)
                                yield (n, p, o)
                        else:
                            o = self._prepare_object(prop, prop_type, value)
                            yield (n, p, o)
                else:
                    s = self.uriref(u)
                    p = self.uriref(data['edge_label'])
                    o = self.uriref(v)
                    yield (s, p, o)
        for t in cache:
            yield (t[0], t[1], t[2])

