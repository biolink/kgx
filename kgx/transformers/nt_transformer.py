import itertools
import logging
from typing import Set, Optional

from rdflib.plugins.parsers.ntriples import NTriplesParser
from rdflib.plugins.serializers.nt import NT11Serializer
from rdflib.term import URIRef, Literal
import networkx as nx

from kgx import RdfTransformer
from kgx.prefix_manager import PrefixManager
from kgx.utils.kgx_utils import get_toolkit
from kgx.utils.rdf_utils import generate_uuid


class NtTransformer(RdfTransformer):
    """
    Transformer that parses n-triples (NT) and loads triples, as nodes and edges, into a networkx.MultiDiGraph

    .. note::
        This is a specialized version of RdfTransformer that doesn't rely on rdflib.Graph when parsing NTs.
        Depending on performance, this implementation will be subsumed into RdfTransformer.

    """

    def __init__(self, source_graph: nx.MultiDiGraph = None, node_properties: Set = None, edge_properties: Set = None):
        super().__init__(source_graph)
        self.toolkit = get_toolkit()
        self.prefix_manger = PrefixManager()
        self.node_properties = node_properties if node_properties else set()
        self.edge_properties = edge_properties if edge_properties else set()
        self.node_properties.update(
            ['biolink:same_as', 'OBAN:association_has_object', 'OBAN:association_has_subject',
             'OBAN:association_has_predicate', 'OBAN:association_has_object'])
        self.edge_properties.update(['biolink:has_modifier', 'biolink:has_gene_product', 'biolink:has_db_xref', 'biolink:in_taxon'])
        self.edge_properties.update(['biolink:subclass_of', 'biolink:same_as', 'biolink:part_of', 'biolink:has_part'])
        self.assocs = set()
        self.count = 0

    def parse(self, filename: str = None, input_format: str = None, provided_by: str = None, predicates: Set[URIRef] = None) -> None:
        """
        Parse a n-triple file into networkx.MultiDiGraph

        The file must be a *.nt formatted file.

        Parameters
        ----------
        filename : str
            File to read from.
        input_format : str
            The input file format;  default: ``nt``
            RDF will be supported in the future.
        provided_by : str
            Define the source providing the input file.

        """
        p = NTriplesParser(self)
        p.parse(open(filename, 'rb'))
        print("Done parsing NT file")
        self.dereify(self.assocs)

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
        predicate = self.prefix_manger.contract(str(p))
        prop = PrefixManager.get_reference(predicate)
        # check if property is a biolink model property
        # TODO: move this to a separate method
        element = self.toolkit.get_element(prop)
        if element is None:
            mapping = self.toolkit.get_by_mapping(predicate)
            element = self.toolkit.get_element(mapping)
        if element:
            if element.is_a == 'association slot' or predicate in self.edge_properties:
                logging.debug(f"property {prop} is an edge property but belongs to a reified node")
                n = self.add_node(s)
                self.add_node_attribute(n, p, o)
                self.assocs.add(n)
            elif element.is_a == 'node property' or predicate in self.node_properties:
                logging.debug(f"property {prop} is a node property")
                n = self.add_node(s)
                self.add_node_attribute(n, p, o)
            else:
                logging.debug(f"property {prop} is a related_to property")
                self.add_edge(s, o, p)
        else:
            logging.debug(f"property {prop} is not a biolink model element")
            if predicate in self.node_properties:
                logging.debug(f"treating {predicate} as node property")
                n = self.add_node(s)
                self.add_node_attribute(n, p, o)
            else:
                # treating as an edge
                logging.debug(f"treating {predicate} as edge property")
                self.add_edge(s, o, p)
        self.count += 1
        if self.count % 1000 == 0:
            logging.info(f"Parsed {self.count} triples")

    def dereify(self, associations: Set[str]) -> None:
        """
        Dereify an association node.

        Parameters
        ----------
        associations: Set[str]
            A set containing nodes to be dereified.

        """
        for n in associations:
            data = self.graph.nodes[n]
            logging.debug(f"Dereifying node {n} {data}")
            subject = data['subject']
            object = data['object']
            predicate = data['predicate']
            edge_data = {'id': n, 'subject': subject, 'object': object, 'edge_label': predicate}
            for k, v in data.items():
                if k not in {'association_has_subject', 'association_has_object', 'association_has_predicate'}:
                    edge_data[k] = v
            self.graph.add_edge(subject, object, n, **edge_data)
            self.graph.remove_node(n)

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
                p = self.uriref(k)
                if isinstance(v, list):
                    for x in v:
                        if isinstance(x, str):
                            if self.prefix_manger.is_curie(x) or x.startswith('urn:uuid:'):
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
                        if self.prefix_manger.is_curie(v) or v.startswith('urn:uuid:'):
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
        for u, v, k, data in self.graph.edges(data=True, keys=True):
            if data['edge_label'] in self.edge_properties:
                # treat as a direct edge
                s = self.uriref(u)
                p = self.uriref(data['edge_label'])
                o = self.uriref(v)
                yield (s, p, o)
            else:
                # reify
                if 'id' in data:
                    s = self.uriref(data['id'])
                else:
                    # generate a UUID for the reified node
                    s = self.uriref(generate_uuid())
                all_data = data.copy()
                all_data['type'] = 'biolink:Association'
                for prop, value in all_data.items():
                    if prop in {'id'}:
                        continue
                    p = self.uriref(prop)
                    if isinstance(value, list):
                        for x in value:
                            if isinstance(x, str) and PrefixManager.is_curie(x):
                                o = self.uriref(x)
                            else:
                                o = Literal(x)
                            yield (s, p, o)
                    else:
                        if isinstance(value, str) and PrefixManager.is_curie(value):
                            o = self.uriref(value)
                        else:
                            # literal
                            o = Literal(value)
                        yield (s, p, o)
