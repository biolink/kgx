import gzip
import ijson
from itertools import chain
from typing import Dict, Tuple, Generator, Optional, Any

from kgx.source.json_source import JsonSource


# TODO: update for TRAPI 1.0 spec


class TrapiSource(JsonSource):
    """
    TrapiSource is responsible for reading data as records
    from a TRAPI JSON.
    """

    def __init__(self):
        super().__init__()
        self._node_properties = set()
        self._edge_properties = set()

    def parse(
        self,
        filename: str,
        format: str = "json",
        compression: Optional[str] = None,
        **kwargs: Any
    ) -> Generator:
        """
        This method reads from a JSON and yields records.

        Parameters
        ----------
        filename: str
            The filename to parse
        format: str
            The format (``trapi-json``)
        compression: Optional[str]
            The compression type (``gz``)
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for node and edge records

        """

        self.set_provenance_map(kwargs)

        n = self.read_nodes(filename, compression)
        e = self.read_edges(filename, compression)
        yield from chain(n, e)

    def read_nodes(self, filename: str, compression: Optional[str] = None) -> Generator:
        """
        Read node records from a JSON.

        Parameters
        ----------
        filename: str
            The filename to read from
        compression: Optional[str]
            The compression type

        Returns
        -------
        Generator
            A generator for node records

        """
        if compression == "gz":
            FH = gzip.open(filename, "rb")
        else:
            FH = open(filename, "rb")
        for n in ijson.items(FH, "knowledge_graph.nodes.item"):
            yield self.load_node(n)

    def read_edges(self, filename: str, compression: Optional[str] = None) -> Generator:
        """
        Read edge records from a JSON.

        Parameters
        ----------
        filename: str
            The filename to read from
        compression: Optional[str]
            The compression type

        Returns
        -------
        Generator
            A generator for edge records

        """
        if compression == "gz":
            FH = gzip.open(filename, "rb")
        else:
            FH = open(filename, "rb")
        for e in ijson.items(FH, "knowledge_graph.edges.item"):
            yield self.load_edge(e)

    def load_node(self, node: Dict) -> Tuple[str, Dict]:
        """
        Load a node into an instance of BaseGraph

        .. Note::
            This method transformers Reasoner Std API format fields to Biolink Model fields.

        Parameters
        ----------
        node : Dict
            A node

        """

        if "type" in node and "category" not in node:
            node["category"] = node["type"]
            del node["type"]
        return super().read_node(node)

    def load_edge(self, edge: Dict) -> Tuple[str, str, str, Dict]:
        """
        Load an edge into an instance of BaseGraph

        .. Note::
            This methods transformers Reasoner Std API format fields to Biolink Model fields.

        Parameters
        ----------
        edge : Dict
            An edge

        """
        if "source_id" in edge:
            edge["subject"] = edge["source_id"]
        if "target_id" in edge:
            edge["object"] = edge["target_id"]
        if "relation_label" in edge:
            edge["predicate"] = edge["relation_label"][0]
        return super().read_edge(edge)
