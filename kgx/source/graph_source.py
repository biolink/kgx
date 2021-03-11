from itertools import chain
from typing import Generator, Any

from kgx.config import get_graph_store_class
from kgx.graph.base_graph import BaseGraph
from kgx.source.source import Source
from kgx.utils.kgx_utils import validate_node, validate_edge, sanitize_import


class GraphSource(Source):
    """
    GraphSource is responsible for reading data as records
    from an in memory graph representation.

    The underlying store must be an instance of ``kgx.graph.base_graph.BaseGraph``
    """

    def __init__(self):
        super().__init__()
        self.graph = get_graph_store_class()()

    def parse(self, graph: BaseGraph, provided_by: str = None, **kwargs: Any) -> Generator:
        """
        This method reads from a graph and yields records.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph to read from
        provided_by: Optional[str]
            The name of the source providing the graph
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for node and edge records read from the graph

        """
        self.graph = graph
        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]
        nodes = self.read_nodes()
        edges = self.read_edges()
        yield from chain(nodes, edges)

    def read_nodes(self) -> Generator:
        """
        Read nodes as records from the graph.

        Returns
        -------
        Generator
            A generator for nodes

        """
        for n, data in self.graph.nodes(data=True):
            if 'id' not in data:
                data['id'] = n
            node_data = validate_node(data)
            node_data = sanitize_import(node_data.copy())
            if 'provided_by' in self.graph_metadata and 'provided_by' not in node_data.keys():
                node_data['provided_by'] = self.graph_metadata['provided_by']
            if self.check_node_filter(node_data):
                self.node_properties.update(node_data.keys())
                yield n, node_data

    def read_edges(self) -> Generator:
        """
        Read edges as records from the graph.

        Returns
        -------
        Generator
            A generator for edges

        """
        for u, v, k, data in self.graph.edges(keys=True, data=True):
            edge_data = validate_edge(data)
            edge_data = sanitize_import(edge_data.copy())
            if 'provided_by' in self.graph_metadata and 'provided_by' not in edge_data.keys():
                edge_data['provided_by'] = self.graph_metadata['provided_by']
            if self.check_edge_filter(edge_data):
                self.node_properties.update(edge_data.keys())
                yield u, v, k, edge_data
