from typing import Dict

from kgx.graph.base_graph import BaseGraph

from kgx.config import get_graph_store_class
from kgx.sink.sink import Sink
from kgx.utils.kgx_utils import generate_edge_key


class GraphSink(Sink):
    """
    GraphSink is responsible for writing data as records
    to an in memory graph representation.

    The underlying store is determined by the graph store
    class defined in config (``kgx.graph.nx_graph.NxGraph``, by default).

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        An instance of BaseGraph to read from

    """

    def __init__(self, graph: BaseGraph = None):
        super().__init__()
        if graph:
            self.graph = graph
        else:
            self.graph = get_graph_store_class()()

    def write_node(self, record: Dict) -> None:
        """
        Write a node record to graph.

        Parameters
        ----------
        record: Dict
            A node record

        """
        self.graph.add_node(record["id"], **record)

    def write_edge(self, record: Dict) -> None:
        """
        Write an edge record to graph.

        Parameters
        ----------
        record: Dict
            An edge record

        """
        key = (
            record["key"]
            if "key" in record
            else generate_edge_key(
                record["subject"], record["predicate"], record["object"]
            )
        )
        self.graph.add_edge(record["subject"], record["object"], key, **record)

    def finalize(self) -> None:
        """
        Perform any operations after writing nodes and edges to graph.
        """
        pass
