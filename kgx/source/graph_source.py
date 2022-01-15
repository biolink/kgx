from itertools import chain
from typing import Generator, Any, Dict, Optional

from kgx.config import get_graph_store_class
from kgx.graph.base_graph import BaseGraph
from kgx.source.source import Source
from kgx.utils.kgx_utils import sanitize_import


class GraphSource(Source):
    """
    GraphSource is responsible for reading data as records
    from an in memory graph representation.

    The underlying store must be an instance of ``kgx.graph.base_graph.BaseGraph``
    """

    def __init__(self, owner):
        super().__init__(owner)
        self.graph = get_graph_store_class()()

    def parse(self, graph: BaseGraph, **kwargs: Any) -> Generator:
        """
        This method reads from a graph and yields records.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph to read from
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for node and edge records read from the graph

        """
        self.graph = graph

        self.set_provenance_map(kwargs)

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
            if "id" not in data:
                data["id"] = n

            node_data = self.validate_node(data)
            if not node_data:
                continue

            node_data = sanitize_import(node_data.copy())

            self.set_node_provenance(node_data)

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

            edge_data = self.validate_edge(data)
            if not edge_data:
                continue

            edge_data = sanitize_import(edge_data.copy())

            self.set_edge_provenance(edge_data)

            if self.check_edge_filter(edge_data):
                self.node_properties.update(edge_data.keys())
                yield u, v, k, edge_data
