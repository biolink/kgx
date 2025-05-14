import os
import tarfile
from typing import Optional, Dict, Set, Any, List
from ordered_set import OrderedSet

from kgx.sink.sink import Sink
from kgx.utils.kgx_utils import (
    extension_types,
    archive_write_mode,
    archive_format,
    build_export_row
)


DEFAULT_NODE_COLUMNS = {
    "id",
    "name",
    "category",
    "description",
    "provided_by",
    "synonym",
    "exact_synonym",
    "related_synonym",
    "narrow_synonym",
    "broad_synonym"
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
DEFAULT_LIST_DELIMITER = "|"


class TsvSink(Sink):
    """
    TsvSink is responsible for writing data as records to a TSV/CSV.

    Parameters
    ----------
    owner: Transformer
        Transformer to which the GraphSink belongs
    filename: str
        The filename to write to
    format: str
        The file format (``tsv``, ``csv``)
    compression: str
        The compression type (``tar``, ``tar.gz``)
    kwargs: Any
        Any additional arguments
    """

    def __init__(
        self,
        owner,
        filename: str,
        format: str,
        compression: Optional[str] = None,
        **kwargs: Any,
    ):
        super().__init__(owner)
        if format not in extension_types:
            raise Exception(f"Unsupported format: {format}")
        self.delimiter = extension_types[format]
        self.dirname = os.path.abspath(os.path.dirname(filename))
        self.basename = os.path.basename(filename)
        self.extension = format.split(":")[0]
        self.mode = (
            archive_write_mode[compression]
            if compression in archive_write_mode
            else None
        )
        self.list_delimiter = kwargs["list_delimiter"] if "list_delimiter" in kwargs else DEFAULT_LIST_DELIMITER
        self.nodes_file_basename = f"{self.basename}_nodes.{self.extension}"
        self.edges_file_basename = f"{self.basename}_edges.{self.extension}"
        if self.dirname:
            os.makedirs(self.dirname, exist_ok=True)
        if "node_properties" in kwargs:
            self.node_properties.update(set(kwargs["node_properties"]))
        else:
            self.node_properties.update(DEFAULT_NODE_COLUMNS)
        if "edge_properties" in kwargs:
            self.edge_properties.update(set(kwargs["edge_properties"]))
        else:
            self.edge_properties.update(DEFAULT_EDGE_COLUMNS)
        self.ordered_node_columns = TsvSink._order_node_columns(self.node_properties)
        self.ordered_edge_columns = TsvSink._order_edge_columns(self.edge_properties)

        self.nodes_file_name = os.path.join(
            self.dirname if self.dirname else "", self.nodes_file_basename
        )
        self.NFH = open(self.nodes_file_name, "w")
        self.NFH.write(self.delimiter.join(self.ordered_node_columns) + "\n")
        self.edges_file_name = os.path.join(
            self.dirname if self.dirname else "", self.edges_file_basename
        )
        self.EFH = open(self.edges_file_name, "w")
        self.EFH.write(self.delimiter.join(self.ordered_edge_columns) + "\n")

    def write_node(self, record: Dict) -> None:
        """
        Write a node record to the underlying store.

        Parameters
        ----------
        record: Dict
            A node record

        """
        row = build_export_row(record, list_delimiter=self.list_delimiter)
        row["id"] = record["id"]
        values = []
        for c in self.ordered_node_columns:
            if c in row:
                values.append(str(row[c]))
            else:
                values.append("")
        self.NFH.write(self.delimiter.join(values) + "\n")

    def write_edge(self, record: Dict) -> None:
        """
        Write an edge record to the underlying store.

        Parameters
        ----------
        record: Dict
            An edge record

        """
        row = build_export_row(record, list_delimiter=self.list_delimiter)
        values = []
        for c in self.ordered_edge_columns:
            if c in row:
                values.append(str(row[c]))
            else:
                values.append("")
        self.EFH.write(self.delimiter.join(values) + "\n")

    def finalize(self) -> None:
        """
        Close file handles and create an archive if compression mode is defined.
        """
        self.NFH.close()
        self.EFH.close()
        if self.mode:
            archive_basename = f"{self.basename}.{archive_format[self.mode]}"
            archive_name = os.path.join(
                self.dirname if self.dirname else "", archive_basename
            )
            with tarfile.open(name=archive_name, mode=self.mode) as tar:
                tar.add(self.nodes_file_name, arcname=self.nodes_file_basename)
                tar.add(self.edges_file_name, arcname=self.edges_file_basename)
                if os.path.isfile(self.nodes_file_name):
                    os.remove(self.nodes_file_name)
                if os.path.isfile(self.edges_file_name):
                    os.remove(self.edges_file_name)

    @staticmethod
    def _order_node_columns(cols: Set) -> OrderedSet:
        """
        Arrange node columns in a defined order.

        Parameters
        ----------
        cols: Set
            A set with elements in any order

        Returns
        -------
        OrderedSet
            A set with elements in a defined order

        """
        node_columns = cols.copy()
        core_columns = OrderedSet(
            ["id", "category", "name", "description", "xref", "provided_by", "synonym", "exact_synonym", "broad_synonym", "narrow_synonym", "related_synonym"]
        )
        ordered_columns = OrderedSet()
        for c in core_columns:
            if c in node_columns:
                ordered_columns.add(c)
                node_columns.remove(c)
        internal_columns = set()
        remaining_columns = node_columns.copy()
        for c in node_columns:
            if c.startswith("_"):
                internal_columns.add(c)
                remaining_columns.remove(c)
        ordered_columns.update(sorted(remaining_columns))
        ordered_columns.update(sorted(internal_columns))
        return ordered_columns

    @staticmethod
    def _order_edge_columns(cols: Set) -> OrderedSet:
        """
        Arrange edge columns in a defined order.

        Parameters
        ----------
        cols: Set
            A set with elements in any order

        Returns
        -------
        OrderedSet
            A set with elements in a defined order

        """
        edge_columns = cols.copy()
        core_columns = OrderedSet(
            [
                "id",
                "subject",
                "predicate",
                "object",
                "category",
                "relation",
                "provided_by",
            ]
        )
        ordered_columns = OrderedSet()
        for c in core_columns:
            if c in edge_columns:
                ordered_columns.add(c)
                edge_columns.remove(c)
        internal_columns = set()
        remaining_columns = edge_columns.copy()
        for c in edge_columns:
            if c.startswith("_"):
                internal_columns.add(c)
                remaining_columns.remove(c)
        ordered_columns.update(sorted(remaining_columns))
        ordered_columns.update(sorted(internal_columns))
        return ordered_columns

    def set_node_properties(self, node_properties: List) -> None:
        """
        Update node properties index with a given list.

        Parameters
        ----------
        node_properties: List
            A list of node properties

        """
        self._node_properties.update(node_properties)
        self.ordered_node_columns = TsvSink._order_node_columns(self._node_properties)

    def set_edge_properties(self, edge_properties: List) -> None:
        """
        Update edge properties index with a given list.

        Parameters
        ----------
        edge_properties: List
            A list of edge properties

        """
        self._edge_properties.update(edge_properties)
        self.ordered_edge_columns = TsvSink._order_edge_columns(self._edge_properties)
