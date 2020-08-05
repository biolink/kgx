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
