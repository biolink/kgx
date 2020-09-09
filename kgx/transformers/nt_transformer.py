import gzip
import itertools
from typing import Set, Optional, Dict

from rdflib.plugins.parsers.ntriples import NTriplesParser
from rdflib.plugins.serializers.nt import NT11Serializer
from rdflib.term import URIRef, Literal
import networkx as nx

from kgx import RdfTransformer
from kgx.config import get_logger
from kgx.utils.kgx_utils import current_time_in_millis, apply_filters

log = get_logger()


class NtTransformer(RdfTransformer):
    """
    Transformer that parses n-triples (NT) and loads triples, as nodes and edges, into a networkx.MultiDiGraph

    .. note::
        This is a specialized version of RdfTransformer that doesn't rely on rdflib.Graph when parsing NTs.
        Depending on performance, this implementation will be subsumed into RdfTransformer.

    """

    def __init__(self, source_graph: nx.MultiDiGraph = None, curie_map: Dict = None):
        super().__init__(source_graph, curie_map)

    def parse(self, filename: str, input_format: Optional[str] = 'nt', compression: Optional[str] = None, provided_by: Optional[str] = None, node_property_predicates: Optional[Set[str]] = None) -> None:
        """
        Parse a n-triple file into networkx.MultiDiGraph

        The file must be a *.nt formatted file.

        Parameters
        ----------
        filename : str
            File to read from.
        input_format : Optional[str]
            The input file format. Must be ``nt``
        compression: Optional[str]
            The compression type. For example, ``gz``
        provided_by : Optional[str]
            Define the source providing the input file.
        node_property_predicates: Optional[Set[str]]
            A set of rdflib.URIRef representing predicates that are to be treated as node properties

        """
        p = NTriplesParser(self)
        if node_property_predicates:
            self.node_properties.update([URIRef(self.prefix_manager.expand(x)) for x in node_property_predicates])

        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]

        self.start = current_time_in_millis()
        if compression == 'gz':
            p.parse(gzip.open(filename, 'rb'))
        else:
            p.parse(open(filename, 'rb'))

        self.dereify(self.reified_nodes)
        log.info(f"Done parsing {filename}")
        apply_filters(self.graph, self.node_filters, self.edge_filters)

    def save(self, filename: str, output_format: str = 'nt', compression: str = None, reify_all_edges = False, **kwargs) -> None:
        """
        Export networkx.MultiDiGraph into n-triple format.

        Uses rdflib.plugins.serializers.nt.NT11Serializer.

        Parameters
        ----------
        filename: str
            Filename to write to
        output_format: str
            The output format. Must be ``nt``
        compression: str
            The compression type. For example, ``gz``
        reify_all_edges: bool
            Whether to reify all edges in the graph
        kwargs: dict
            Any additional arguments

        """
        nodes_generator = self.export_nodes()
        edges_generator = self.export_edges(reify_all_edges)
        generator = itertools.chain(nodes_generator, edges_generator)
        serializer = NT11Serializer(generator)
        if compression == 'gz':
            f = gzip.open(filename, 'wb')
        else:
            f = open(filename, 'wb')
        serializer.serialize(f)
