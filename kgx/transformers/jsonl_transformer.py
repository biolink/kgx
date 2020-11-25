import gzip
import re
from typing import Optional
import jsonlines as jsonlines

from kgx import JsonTransformer
from kgx.config import get_logger
from kgx.graph.base_graph import BaseGraph


log = get_logger()


class JsonlTransformer(JsonTransformer):
    """
    Transformer that parsers JSONLines (*.jsonl), and loads nodes and edges
    into an instance of BaseGraph

    Parameters
    ----------
    source_graph: Optional[kgx.graph.base_graph.BaseGraph]
        The source graph

    """
    def __init__(self, source_graph: Optional[BaseGraph] = None):
        super().__init__(source_graph)

    def parse(self, filename: str, input_format: str = 'jsonl', compression: Optional[str] = None, provided_by: Optional[str] = None, **kwargs) -> None:
        """
        Parse jsonl files.

        .. note::
            This is a specialized version of JsonTransformer that supports reading and writing jsonlines.
            Due to the nature of the jsonl format, this method expects nodes and edges to be in separate files.

        Parameters
        ----------
        filename: str
            JSON file to read from
        input_format: str
            The input file format (``jsonl``, by default)
        compression: Optional[str]
            The compression type. For example, ``gz``
        provided_by: Optional[str]
            Define the source providing the input file
        kwargs: dict
            Any additional arguments
        """
        log.info("Parsing {}".format(filename))
        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]
        if re.search(f'nodes.{input_format}', filename):
            m = self.load_node # type: ignore
        elif re.search(f'edges.{input_format}', filename):
            m = self.load_edge # type: ignore
        else:
            raise TypeError(f"Unrecognized file: {filename}")

        if compression == 'gz':
            with gzip.open(filename, 'rb') as FH:
                reader = jsonlines.Reader(FH)
                for obj in reader:
                    m(obj)
        else:
            with jsonlines.open(filename) as FH:
                for obj in FH:
                    m(obj)

    def save(self, filename: str, output_format: str = 'jsonl', compression: Optional[str] = None, **kwargs) -> str:
        """
        Write kgx.graph.base_graph.BaseGraph to jsonl.
        This method writes nodes to ``*nodes.jsonl`` and edges to ``edges.jsonl``

        Parameters
        ----------
        filename: str
            Filename to write to
        output_format: str
            The output file format (``jsonl``, by default)
        compression: Optional[str]
            The compression type. For example, ``gz``
        kwargs: dict
            Any additional arguments

        Returns
        -------
        str
            The filename

        """
        nodes_filename = f"{filename}_nodes.jsonl"
        edges_filename = f"{filename}_edges.jsonl"
        if compression == 'gz':
            nodes_filename += f".{compression}"
            edges_filename += f".{compression}"
            with gzip.open(nodes_filename, 'wb') as WH:
                writer = jsonlines.Writer(WH)
                for n, data in self.graph.nodes(data=True):
                    writer.write(data)
            with gzip.open(edges_filename, 'wb') as WH:
                writer = jsonlines.Writer(WH)
                for u, v, k, data in self.graph.edges(data=True, keys=True):
                    writer.write(data)
        else:
            with jsonlines.open(nodes_filename, 'w') as WH:
                for n, data in self.graph.nodes(data=True):
                    WH.write(data)
            with jsonlines.open(edges_filename, 'w') as WH:
                for u, v, k, data in self.graph.edges(data=True, keys=True):
                    WH.write(data)
        return filename
