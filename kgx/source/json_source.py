import gzip
from typing import Optional, Generator, Any
from pprint import pprint
import ijson
from itertools import chain
from typing import Dict, Tuple, Any, Generator, Optional, List
from kgx.config import get_logger
from kgx.error_detection import ErrorType, MessageLevel
from kgx.source.source import Source
from kgx.utils.kgx_utils import (
    generate_uuid,
    generate_edge_key,
    extension_types,
    archive_read_mode,
    sanitize_import
)
from kgx.source.tsv_source import TsvSource
log = get_logger()


class JsonSource(TsvSource):
    """
    JsonSource is responsible for reading data as records
    from a JSON.
    """

    def __init__(self, owner):
        super().__init__(owner)
        self.compression = None
        self.list_delimiter = None

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
            The format (``json``)
        compression: Optional[str]
            The compression type (``gz``)
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for node and edge records read from the file

        """

        self.set_provenance_map(kwargs)

        self.compression = compression
        n = self.read_nodes(filename)
        e = self.read_edges(filename)
        yield from chain(n, e)

    def read_nodes(self, filename: str) -> Generator:
        """
        Read node records from a JSON.

        Parameters
        ----------
        filename: str
            The filename to read from

        Returns
        -------
        Generator
            A generator for node records

        """
        if self.compression == "gz":
            FH = gzip.open(filename, "rb")
        else:
            FH = open(filename, "rb")
        for n in ijson.items(FH, "nodes.item"):
            yield self.read_node(n)

    def read_edges(self, filename: str) -> Generator:
        """
        Read edge records from a JSON.

        Parameters
        ----------
        filename: str
            The filename to read from

        Returns
        -------
        Generator
            A generator for edge records

        """
        if self.compression == "gz":
            FH = gzip.open(filename, "rb")
        else:
            FH = open(filename, "rb")
        for e in ijson.items(FH, "edges.item", use_float=True):
            yield self.read_edge(e)

    def read_edge(self, edge: Dict) -> Optional[Tuple]:
        """
        Load an edge into an instance of BaseGraph.

        Parameters
        ----------
        edge: Dict
            An edge

        Returns
        -------
        Optional[Tuple]
            A tuple that contains subject id, object id, edge key, and edge data

        """
        edge = self.validate_edge(edge)
        if not edge:
            return None
        edge_data = sanitize_import(edge.copy())
        log.debug("after sanitize, edge_data looks like this")
        log.debug(edge_data)
        if "id" not in edge_data:
            edge_data["id"] = generate_uuid()
        s = edge_data["subject"]
        o = edge_data["object"]

        self.set_edge_provenance(edge_data)

        key = generate_edge_key(s, edge_data["predicate"], o)
        self.edge_properties.update(list(edge_data.keys()))
        log.debug(self.edge_properties)
        if self.check_edge_filter(edge_data):
            self.node_properties.update(edge_data.keys())
            return s, o, key, edge_data
