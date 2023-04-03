import gzip
import typing
import ijson
from itertools import chain
from typing import Dict, Tuple, Any, Generator, Optional, List
from kgx.config import get_logger

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
    ) -> typing.Generator:
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

