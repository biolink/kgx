import gzip
import os
from typing import Optional, Dict, Any

import jsonlines

from kgx.sink.sink import Sink


class JsonlSink(Sink):
    """
    JsonlSink is responsible for writing data as records
    to JSON lines.

    Parameters
    ----------
    owner: Transformer
        Transformer to which the GraphSink belongs
    filename: str
        The filename to write to
    format: str
        The file format (``jsonl``)
    compression: Optional[str]
        The compression type (``gz``)
    kwargs: Any
        Any additional arguments

    """

    def __init__(
        self,
        owner,
        filename: str,
        format: str = "jsonl",
        compression: Optional[str] = None,
        **kwargs: Any,
    ):
        super().__init__(owner)
        dirname = os.path.abspath(os.path.dirname(filename))
        basename = os.path.basename(filename)
        nodes_filename = os.path.join(
            dirname if dirname else "", f"{basename}_nodes.{format}"
        )
        edges_filename = os.path.join(
            dirname if dirname else "", f"{basename}_edges.{format}"
        )
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        if compression == "gz":
            nodes_filename += f".{compression}"
            edges_filename += f".{compression}"
            NFH = gzip.open(nodes_filename, "wb")
            self.NFH = jsonlines.Writer(NFH)
            EFH = gzip.open(edges_filename, "wb")
            self.EFH = jsonlines.Writer(EFH)
        else:
            self.NFH = jsonlines.open(nodes_filename, "w")
            self.EFH = jsonlines.open(edges_filename, "w")

    def write_node(self, record: Dict) -> None:
        """
        Write a node record to JSON.

        Parameters
        ----------
        record: Dict
            A node record

        """
        self.NFH.write(record)

    def write_edge(self, record: Dict) -> None:
        """
        Write an edge record to JSON.

        Parameters
        ----------
        record: Dict
            A node record

        """
        self.EFH.write(record)

    def finalize(self) -> None:
        """
        Perform any operations after writing the file.
        """
        self.NFH.close()
        self.EFH.close()
