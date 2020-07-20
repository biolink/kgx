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
from kgx.utils.kgx_utils import get_toolkit, current_time_in_millis, get_biolink_node_properties
from kgx.utils.rdf_utils import generate_uuid

INPUT_FORMATS = ['nt', 'nt.gz']


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
        print(f"All node predicates: {self.node_properties}")
        self.start = current_time_in_millis()
        if input_format == INPUT_FORMATS[0]:
            p.parse(open(filename, 'rb'))
        elif input_format == INPUT_FORMATS[1]:
            p.parse(gzip.open(filename, 'rb'))
        else:
            raise NameError(f"input_format: {input_format} not supported. Must be one of {INPUT_FORMATS}")
        print(f"Done parsing {filename}")
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
        if p in self.cache:
            # already processed this predicate before; pull from cache
            element = self.cache[p]['element']
            predicate = self.cache[p]['predicate']
            property_name = self.cache[p]['property_name']
        else:
            # haven't seen this predicate before; map to element
            predicate = self.prefix_manager.contract(str(p))
            property_name = self.prefix_manager.get_reference(predicate)
            element = self.get_biolink_element(predicate, property_name)
            self.cache[p] = {'element': element, 'predicate': predicate, 'property_name': property_name}

        if element:
            if element.is_a == 'association slot':
                logging.debug(f"property {property_name} is an edge property but belongs to a reified node")
                self.add_node_attribute(s, p, o)
                self.reified_nodes.add(s)
            elif element.is_a == 'node property' or predicate in self.node_properties:
                logging.debug(f"property {property_name} is a node property")
                self.add_node_attribute(s, p, o)
            else:
                logging.debug(f"adding property {property_name} as an edge")
                self.add_edge(s, o, p)
        else:
            logging.debug(f"property {property_name} is not a biolink model element")
            if predicate in self.node_properties:
                logging.debug(f"treating {predicate} as node property")
                self.add_node_attribute(s, p, o)
            else:
                # treating as an edge
                logging.debug(f"treating {predicate} as edge")
                self.add_edge(s, o, p)
        self.count += 1
        if self.count % 1000 == 0:
            logging.info(f"Parsed {self.count} triples; time taken: {current_time_in_millis() - self.start} ms")
            self.start = current_time_in_millis()

    def save(self, filename: str = None, output_format: str = "nt", **kwargs) -> None:
        """
        Export networkx.MultiDiGraph into n-triple format.

        Uses rdflib.plugins.serializers.nt.NT11Serializer.

        Parameters
        ----------
        filename: str
            Filename to write to
        output_format: str
            The output format; default: ``nt``
        kwargs: dict
            Any additional arguments

        """
        nodes_generator = self.export_nodes()
        edges_generator = self.export_edges()
        generator = itertools.chain(nodes_generator, edges_generator)
        serializer = NT11Serializer(generator)
        serializer.serialize(open(filename, 'wb'))

    def export_nodes(self) -> Set[URIRef]:
        """
        Export all nodes from networkx.MultiDiGraph.

        This method yields one (or more) triple that corresponds to a node.

        Returns
        -------
        Set[rdflib.term.URIRef]
            A triple

        """
        for n, data in self.graph.nodes(data=True):
            s = self.uriref(n)
            for k, v in data.items():
                if k in {'id', 'iri'}:
                    continue
                p = self.uriref(k)
                if isinstance(v, list):
                    for x in v:
                        if isinstance(x, str):
                            if self.prefix_manager.is_curie(x) or self.prefix_manager.is_iri(x) or x.startswith('urn:uuid:'):
                                o = self.uriref(x)
                            else:
                                # literal
                                o = Literal(x)
                        else:
                            # literal
                            o = Literal(x)
                        yield (s, p, o)
                else:
                    if isinstance(v, str):
                        if self.prefix_manager.is_curie(v) or self.prefix_manager.is_iri(v) or v.startswith('urn:uuid:'):
                            o = self.uriref(v)
                        else:
                            # literal
                            o = Literal(v)
                    else:
                        # literal
                        o = Literal(v)
                    yield (s, p, o)

    def export_edges(self) -> Set[URIRef]:
        """
        Export all edges from networkx.MultiDiGraph.

        This method yields one (or more) triple that corresponds to an edge.

        Returns
        -------
        Set[rdflib.term.URIRef]
            A triple

        """
        cache = []
        for u, v, k, data in self.graph.edges(data=True, keys=True):
            if data['edge_label'] in self.edge_properties:
                # treat as a direct edge
                s = self.uriref(u)
                p = self.uriref(data['edge_label'])
                o = self.uriref(v)
                yield (s, p, o)
            else:
                # reify
                s = self.uriref(u)
                p = self.uriref(data['edge_label'])
                o = self.uriref(v)
                cache.append((s, p, o))
                if 'id' in data:
                    s = self.uriref(data['id'])
                else:
                    # generate a UUID for the reified node
                    s = self.uriref(generate_uuid())
                all_data = data.copy()
                all_data['type'] = 'biolink:Association'
                for prop, value in all_data.items():
                    if prop in {'id', 'association_id', 'edge_key'}:
                        continue
                    p = self.uriref(prop)
                    if isinstance(value, list):
                        for x in value:
                            if isinstance(x, str) and PrefixManager.is_curie(x):
                                o = self.uriref(x)
                            elif isinstance(x, str) and PrefixManager.is_iri(x):
                                o = URIRef(x)
                            else:
                                o = Literal(x)
                            yield (s, p, o)
                    else:
                        if isinstance(value, str) and PrefixManager.is_curie(value):
                            o = self.uriref(value)
                        elif isinstance(value, str) and PrefixManager.is_iri(value):
                            o = URIRef(value)
                        else:
                            # literal
                            o = Literal(value)
                        yield (s, p, o)

        for t in cache:
            yield (t[0], t[1], t[2])
