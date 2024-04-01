from typing import Any
from kgx.sink import Sink


class ParquetSink(Sink):
    """
    A ParquetSink writes data to a Parquet file.

    Parameters
    ----------
    owner: Transformer
        Transformer to which the ParquetSink belongs
    filename: str
        Name of the Parquet file to write to
    n/a (**kwargs allowed, but ignored)
    """

    def __init__(self, owner, filename: str, **kwargs: Any):
        super().__init__(owner)
        self.filename = filename
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
        Operations that ought to be done after
        writing all the incoming data should be called
        by this method.

        """
        import pandas as pd
        from pyarrow import Table
        from pyarrow.parquet import write_table
        from kgx.transformer import Transformer

        nodes_df = pd.DataFrame(self.nodes)
        edges_df = pd.DataFrame(self.edges)

        nodes_table = Table.from_pandas(nodes_df)
        edges_table = Table.from_pandas(edges_df)

        write_table(nodes_table, self.filename + '_nodes.parquet')
        write_table(edges_table, self.filename + '_edges.parquet')

        self.nodes = []
        self.edges = []
        self.owner.logger.info(f'Wrote {len(nodes_df)} nodes and {len(edges_df)} edges to {self.filename}')
        self.owner.logger.info(f'Nodes written to {self.filename}_nodes.parquet')
        self.owner.logger.info(f'Edges written to {self.filename}_edges.parquet')