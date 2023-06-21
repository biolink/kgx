
from typing import Dict, Set, Any, List
from ordered_set import OrderedSet
import sqlite3
from kgx.sink.sink import Sink
from kgx.utils.kgx_utils import (
    extension_types,
    build_export_row,
    create_connection,
    get_toolkit,
    sentencecase_to_snakecase,
)
from kgx.config import get_logger

log = get_logger()
DEFAULT_NODE_COLUMNS = {"id", "name", "category", "description", "provided_by"}
DEFAULT_EDGE_COLUMNS = {
    "id",
    "subject",
    "predicate",
    "object",
    "relation",
    "category",
    "knowledge_source",
}

# TODO: incorporate closurizer, add denormalizer method?
# TODO: add denormalization options to config


class SqlSink(Sink):
    """
    SqlSink is responsible for writing data as records to a SQLlite DB.

    Parameters
    ----------
    owner: Transformer
        Transformer to which the GraphSink belongs
    filename: str
        The filename to write to
    format: str
        The file format (sqllite, tsv?)
    kwargs: Any
        Any additional arguments
    """

    def __init__(
        self,
        owner,
        filename: str,
        format: str,
        **kwargs: Any,
    ):
        super().__init__(owner)
        if format not in extension_types:
            raise Exception(f"Unsupported format: {format}")
        self.conn = create_connection(filename)
        self.edge_data = []
        self.node_data = []
        self.filename = filename
        if "node_properties" in kwargs:
            self.node_properties.update(set(kwargs["node_properties"]))
        else:
            self.node_properties.update(DEFAULT_NODE_COLUMNS)
        if "edge_properties" in kwargs:
            self.edge_properties.update(set(kwargs["edge_properties"]))
        else:
            self.edge_properties.update(DEFAULT_EDGE_COLUMNS)
        self.ordered_node_columns = SqlSink._order_node_columns(self.node_properties)
        self.ordered_edge_columns = SqlSink._order_edge_columns(self.edge_properties)
        if "node_table_name" in kwargs:
            self.node_table_name = kwargs["node_table_name"]
        else:
            self.node_table_name = "nodes"
        if "edge_table_name" in kwargs:
            self.edge_table_name = kwargs["edge_table_name"]
        else:
            self.edge_table_name = "edges"
        if "denormalize" in kwargs:
            self.denormalize = kwargs["denormalize"]
        else:
            self.denormalize = False
        self.create_tables()

    def create_tables(self):
        # Create the nodes table if it does not already exist
        try:
            if self.ordered_node_columns:
                c = self.conn.cursor()
                columns_str = ', '.join([f'{column} TEXT' for column in self.ordered_node_columns])
                create_table_sql = f'CREATE TABLE IF NOT EXISTS {self.node_table_name} ({columns_str});'
                log.info(create_table_sql)
                c.execute(create_table_sql)
                self.conn.commit()
        except sqlite3.Error as e:
            log.error(f"Error occurred while creating nodes table: {e}", "rolling back")
            self.conn.rollback()

        # Create the edges table if it does not already exist
        try:
            if self.ordered_edge_columns:
                if self.denormalize:
                    tk = get_toolkit()
                    denormalized_slots = tk.get_denormalized_association_slots(formatted=False)
                    for slot in denormalized_slots:
                        self.ordered_edge_columns.append(sentencecase_to_snakecase(slot))
                c = self.conn.cursor()
                columns_str = ', '.join([f'{column} TEXT' for column in self.ordered_edge_columns])
                create_table_sql = f'CREATE TABLE IF NOT EXISTS {self.edge_table_name} ({columns_str});'
                c.execute(create_table_sql)
                self.conn.commit()
        except sqlite3.Error as e:
            log.error(f"Error occurred while creating edges table: {e}", "rolling back")
            self.conn.rollback()
        self.conn.commit()

    def write_node(self, record: Dict) -> None:
        """
        Write a node record to the underlying store.

        Parameters
        ----------
        record: Dict
            A node record

        """
        row = build_export_row(record, list_delimiter=",")
        row["id"] = record["id"]
        values = []
        for c in self.ordered_node_columns:
            if c in row:
                values.append(str(row[c]))
            else:
                values.append("")
        ordered_tuple = tuple(values)
        self.node_data.append(ordered_tuple)

    def write_edge(self, record: Dict) -> None:
        """
        Write an edge record to a tuple list for bulk insert in finalize.

        Parameters
        ----------
        record: Dict
            An edge record

        """
        row = build_export_row(record, list_delimiter="|")
        if self.denormalize:
            self._denormalize_edge(row)
        values = []
        for c in self.ordered_edge_columns:
            if c in row:
                values.append(str(row[c]))
            else:
                values.append("")
        ordered_tuple = tuple(values)
        self.edge_data.append(ordered_tuple)

    def finalize(self) -> None:
        self._bulk_insert(self.node_table_name, self.node_data)
        self._bulk_insert(self.edge_table_name, self.edge_data)
        self._create_indexes()
        self.conn.close()

    def _create_indexes(self):
        c = self.conn.cursor()
        try:
            c.execute(f"CREATE INDEX IF NOT EXISTS node_id_index ON {self.node_table_name} (id);")
            log.info("created index: " + f"CREATE INDEX IF NOT EXISTS node_id_index ON {self.node_table_name} (id);")
            c.execute(f"CREATE INDEX IF NOT EXISTS edge_unique_id_index ON {self.edge_table_name} (subject, predicate, object);")
            log.info("created index: " + f"CREATE INDEX IF NOT EXISTS edge_unique_id_index ON {self.edge_table_name} (subject, predicate, object);")
            self.conn.commit()
        except sqlite3.Error as e:
            log.error(f"Error occurred while creating indexes", {e})
            self.conn.rollback()
        self.conn.commit()

    def _bulk_insert(self, table_name: str, data_list: List[Dict]):
        c = self.conn.cursor()

        # Get the column names in the order they appear in the table
        c.execute(f"SELECT * FROM {table_name}")
        cols = [description[0] for description in c.description]

        # Insert the rows into the table
        query = f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"
        try:
            c.executemany(query, data_list)
            self.conn.commit()
        except sqlite3.Error as e:
            log.error(f"Error occurred while inserting data into table: {e}")
            self.conn.rollback()

    def _denormalize_edge(self, row: dict):
        """
        Add the denormalized node properties to the edge.

        Parameters
        ----------
        row: Dict
            An edge record

        """
        pass
        # subject = row['subject']
        # print(self.node_properties)

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
            ["id", "category", "name", "description", "xref", "provided_by", "synonym"]
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
        self.ordered_node_columns = SqlSink._order_node_columns(self._node_properties)

    def set_edge_properties(self, edge_properties: List) -> None:
        """
        Update edge properties index with a given list.

        Parameters
        ----------
        edge_properties: List
            A list of edge properties

        """
        self._edge_properties.update(edge_properties)
        self.ordered_edge_columns = SqlSink._order_edge_columns(self._edge_properties)
