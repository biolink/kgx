'''Sink for Parquet format.'''

from pathlib import Path
from typing import Any

import pandas as pd
from pyarrow import Table
from pyarrow.parquet import write_table

from kgx.sink.sink import Sink


DEFAULT_NODE_COLUMNS = {
    "id",
    "name",
    "category",
    "description",
    "provided_by"
}
DEFAULT_EDGE_COLUMNS = {
    "id",
    "subject",
    "predicate",
    "object",
    "relation",
    "category",
    "knowledge_source",
}


class ParquetSink(Sink):
    """
    A ParquetSink writes data to Parquet files.

    Parameters
    ----------
    owner: Transformer
        Transformer to which the ParquetSink belongs
    filename: str
        Name of the Parquet file to write to
    kwargs: Any
        Any additional arguments
    """

    def __init__(
            self,
            owner,
            filename: str,
            **kwargs: Any
    ):
        super().__init__(owner)
        self.filename = filename
        self.file_path = Path(filename).resolve()
        self.dirname = self.file_path.parent
        self.basename = self.file_path.stem
        self.nodes_file_basename = f"{self.basename}_nodes.parquet"
        self.edges_file_basename = f"{self.basename}_edges.parquet"

        self.dirname.mkdir(parents=True, exist_ok=True)

        self.nodes_file_name = self.dirname / self.nodes_file_basename
        self.edges_file_name = self.dirname / self.edges_file_basename

        if "node_properties" in kwargs:
            self.node_properties.update(set(kwargs["node_properties"]))
        else:
            self.node_properties.update(DEFAULT_NODE_COLUMNS)
        if "edge_properties" in kwargs:
            self.edge_properties.update(set(kwargs["edge_properties"]))
        else:
            self.edge_properties.update(DEFAULT_EDGE_COLUMNS)

        self.nodes = []
        self.edges = []

    def write_node(self, record) -> None:
        """
        Write a node record to the underlying store.

        Parameters
        ----------
        record: Any
            A node record

        """
        self.nodes.append(record)

    def write_edge(self, record) -> None:
        """
        Write an edge record to the underlying store.

        Parameters
        ----------
        record: Any
            An edge record

        """
        self.edges.append(record)

    def finalize(self) -> None:
        """
        Finalize writing the data to the underlying store.
        """

        nodes_df = pd.DataFrame(self.nodes)
        edges_df = pd.DataFrame(self.edges)

        nodes_table = Table.from_pandas(nodes_df)
        edges_table = Table.from_pandas(edges_df)

        write_table(nodes_table, self.nodes_file_name)
        write_table(edges_table, self.edges_file_name)

        self.nodes = []
        self.edges = []
