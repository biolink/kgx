from itertools import chain
from typing import Generator

from kgx.config import get_graph_store_class
from kgx.graph.base_graph import BaseGraph
from kgx.source.source import Source


class GraphSource(Source):
    """
    A Source is responsible for reading data as records
    from an in memory graph representation.

    The underlying store must be an instance of ``kgx.graph.base_graph.BaseGraph``
    """

    def __init__(self):
        super().__init__()
        self.graph = get_graph_store_class()()

    def parse(self, graph: BaseGraph) -> Generator:
        """
        This method reads from a graph and yields records.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph to read from

        """
        self.graph = graph
        nodes = self.read_nodes()
        edges = self.read_edges()
        yield from chain(nodes, edges)

    def read_nodes(self) -> Generator:
        """
        Read nodes as records from the graph.
        """
        for n, data in self.graph.nodes(data=True):
            yield n, data

    def read_edges(self) -> Generator:
        """
        Read nodes as records from the graph.
        """
        for u, v, k, data in self.graph.edges(keys=True, data=True):
            yield u, v, k, data
