import gzip
import re
import typing

import jsonlines
from typing import Optional, Any, Generator, Dict

from kgx.config import get_logger

log = get_logger()

from kgx.source.json_source import JsonSource


class JsonlSource(JsonSource):
    """
    JsonlSource is responsible for reading data as records
    from JSON Lines.
    """

    def __init__(self, owner):
        super().__init__(owner)

    def parse(
        self,
        filename: str,
        format: str = "jsonl",
        compression: Optional[str] = None,
        **kwargs: Any,
    ) -> typing.Generator:
        """
        This method reads from JSON Lines and yields records.

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
            A generator for records

        """

        self.set_provenance_map(kwargs)

        if re.search(f"nodes.{format}", filename):
            m = self.read_node
        elif re.search(f"edges.{format}", filename):
            m = self.read_edge
        else:
            # This used to throw an exception but perhaps we should simply ignore it.
            log.warning(
                f"Parse function cannot resolve the KGX file type in name {filename}. Skipped..."
            )
            return

        if compression == "gz":
            with gzip.open(filename, "rb") as FH:
                reader = jsonlines.Reader(FH)
                for obj in reader:
                    yield m(obj)
        else:
            with jsonlines.open(filename) as FH:
                for obj in FH:
                    yield m(obj)
