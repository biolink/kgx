import gzip
import re
import jsonlines
from typing import Optional, Any, Generator

from kgx.source.json_source import JsonSource


class JsonlSource(JsonSource):
    """
    JsonlSource is responsible for reading data as records
    from JSON Lines.
    """

    def __init__(self):
        super().__init__()

    def parse(
        self,
        filename: str,
        format: str = 'jsonl',
        compression: Optional[str] = None,
        provenance: Dict[str, str] = dict(),
        **kwargs: Any,
    ) -> Generator:
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
        provenance: Dict[str, str]
            Dictionary of knowledge sources providing the input file
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for records

        """

        if provenance:
            for ksf in provenance.keys():
                self.graph_metadata[ksf] = [provenance[ksf]]

        if re.search(f'nodes.{format}', filename):
            m = self.read_node
        elif re.search(f'edges.{format}', filename):
            m = self.read_edge
        else:
            raise TypeError(f"Unrecognized file: {filename}")

        if compression == 'gz':
            with gzip.open(filename, 'rb') as FH:
                reader = jsonlines.Reader(FH)
                for obj in reader:
                    yield m(obj)
        else:
            with jsonlines.open(filename) as FH:
                for obj in FH:
                    yield m(obj)
