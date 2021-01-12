import os
import re
import pandas as pd
import numpy as np
import tarfile
from ordered_set import OrderedSet

from kgx.config import get_logger
from kgx.graph.base_graph import BaseGraph
from kgx.utils.kgx_utils import generate_edge_key, generate_uuid
from kgx.transformers.transformer import Transformer

from typing import List, Dict, Optional, Any, Set

LIST_DELIMITER = '|'

_column_types = {
    'publications': list,
    'qualifiers': list,
    'category': list,
    'synonym': list,
    'provided_by': list,
    'same_as': list,
    'negated': bool,
    'xrefs': list
}

_extension_types = {
    'csv': ',',
    'tsv': '\t',
    'csv:neo4j': ',',
    'tsv:neo4j': '\t'
}

_archive_read_mode = {
    'tar': 'r',
    'tar.gz': 'r:gz',
    'tar.bz2': 'r:bz2'
}
_archive_write_mode = {
    'tar': 'w',
    'tar.gz': 'w:gz',
    'tar.bz2': 'w:bz2'
}

_archive_format = {
    'r': 'tar',
    'r:gz': 'tar.gz',
    'r:bz2': 'tar.bz2',
    'w': 'tar',
    'w:gz': 'tar.gz',
    'w:bz2': 'tar.bz2'
}

log = get_logger()


class PandasTransformer(Transformer):
    """
    Transformer that parses a TSV/CSV, and loads nodes and edges
    into an instance of kgx.graph.base_graph.BaseGraph

    Parameters
    ----------
    source_graph: Optional[kgx.graph.base_graph.BaseGraph]
        The source graph

    """

    def __init__(self, source_graph: Optional[BaseGraph] = None):
        super().__init__(source_graph)
        self._node_properties: Set = set()
        self._edge_properties: Set = set()

    def parse(self, filename: str, input_format: str = 'tsv', compression: Optional[str] = None, provided_by: Optional[str] = None, **kwargs: Dict) -> None:
        """
        Parse a CSV/TSV (or plain text) file.

        The file can represent either nodes (nodes.tsv) or edges (edges.tsv) or both (data.tar),
        where the tar archive contains nodes.tsv and edges.tsv

        The file can also be data.tar.gz or data.tar.bz2

        Parameters
        ----------
        filename: str
            File to read from
        input_format: str
            The input file format (``tsv``, by default)
        compression: Optional[str]
            The compression. For example, ``tar``
        provided_by: Optional[str]
            Define the source providing the input file
        kwargs: Dict
            Any additional arguments

        """
        if 'delimiter' not in kwargs:
            # infer delimiter from file format
            kwargs['delimiter'] = _extension_types[input_format] # type: ignore
        if 'lineterminator' not in kwargs:
            # set '\n' to be the default line terminator to prevent
            # truncation of lines due to hidden/escaped carriage returns
            kwargs['lineterminator'] = '\n' # type: ignore

        mode = _archive_read_mode[compression] if compression in _archive_read_mode else None

        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]
        if input_format == 'tsv':
            kwargs['quoting'] = 3 # type: ignore
        if mode:
            with tarfile.open(filename, mode=mode) as tar:
                for member in tar.getmembers():
                    f = tar.extractfile(member)
                    file_iter = pd.read_csv(f, dtype=str, chunksize=10000, low_memory=False, keep_default_na=False, **kwargs)
                    if re.search(f'nodes.{input_format}', member.name):
                        for chunk in file_iter:
                            self.load_nodes(chunk)
                    elif re.search(f'edges.{input_format}', member.name):
                        for chunk in file_iter:
                            self.load_edges(chunk)
                    else:
                        raise Exception(f'Tar archive contains an unrecognized file: {member.name}')
        else:
            file_iter = pd.read_csv(filename, dtype=str, chunksize=10000, low_memory=False, keep_default_na=False, **kwargs)
            if re.search(f'nodes.{input_format}', filename):
                for chunk in file_iter:
                    self.load_nodes(chunk)
            elif re.search(f'edges.{input_format}', filename):
                for chunk in file_iter:
                    self.load_edges(chunk)
            else:
                raise Exception(f'Unrecognized file: {filename}')

    def load_nodes(self, df: pd.DataFrame) -> None:
        """
        Load nodes from pandas.DataFrame into an instance of BaseGraph

        Parameters
        ----------
        df : pandas.DataFrame
            Dataframe containing records that represent nodes

        """
        for obj in df.to_dict('records'):
            self.load_node(obj)

    def check_node_filter(self, node: Dict) -> bool:
        """
        Check if a node passes defined node filters.

        Parameters
        ----------
        node: Dict
            A node

        Returns
        -------
        bool
            Whether the given node has passed all defined node filters

        """
        pass_filter = False
        if self.node_filters:
            for k, v in self.node_filters.items():
                if k in node:
                    # filter key exists in node
                    if isinstance(v, (list, set, tuple)):
                        if any(x in node[k] for x in v):
                            pass_filter = True
                        else:
                            return False
                    elif isinstance(v, str):
                        if node[k] == v:
                            pass_filter = True
                        else:
                            return False
                    else:
                        log.error(f"Unexpected {k} node filter of type {type(v)}")
                        return False
                else:
                    # filter key does not exist in node
                    return False
        else:
            # no node filters defined
            pass_filter = True
        return pass_filter

    def load_node(self, node: Dict) -> None:
        """
        Load a node into an instance of BaseGraph

        Parameters
        ----------
        node : Dict
            A node

        """
        if self.check_node_filter(node):
            node = Transformer.validate_node(node)
            kwargs = PandasTransformer._build_kwargs(node.copy())
            if 'id' in kwargs:
                n = kwargs['id']
                if 'provided_by' in self.graph_metadata and 'provided_by' not in kwargs.keys():
                    kwargs['provided_by'] = self.graph_metadata['provided_by']
                self.graph.add_node(n, **kwargs)
                self._node_properties.update(list(kwargs.keys()))
            else:
                log.info("Ignoring node with no 'id': {}".format(node))
        else:
            log.debug(f"Node fails node filters: {node}")

    def load_edges(self, df: pd.DataFrame) -> None:
        """
        Load edges from pandas.DataFrame into an instance of BaseGraph

        Parameters
        ----------
        df : pandas.DataFrame
            Dataframe containing records that represent edges

        """
        for obj in df.to_dict('records'):
            self.load_edge(obj)

    def check_edge_filter(self, edge: Dict) -> bool:
        """
        Check if an edge passes defined edge filters.

        Parameters
        ----------
        edge: Dict
            An edge

        Returns
        -------
        bool
            Whether the given edge has passed all defined edge filters
        """
        pass_filter = False
        if self.edge_filters:
            for k, v in self.edge_filters.items():
                if k in {'subject_category', 'object_category'}:
                    continue
                if k in edge:
                    # filter key exists in edge
                    if isinstance(v, (list, set, tuple)):
                        if any(x in edge[k] for x in v):
                            pass_filter = True
                        else:
                            return False
                    elif isinstance(v, str):
                        if edge[k] == v:
                            pass_filter = True
                        else:
                            return False
                    else:
                        log.error(f"Unexpected {k} edge filter of type {type(v)}")
                        return False
                else:
                    # filter does not exist in edge
                    return False

            # Check for subject and object filter
            if self.graph.has_node(edge['subject']):
                subject_node = self.graph.nodes()[edge['subject']]
            else:
                subject_node = None

            if self.graph.has_node(edge['object']):
                object_node = self.graph.nodes()[edge['object']]
            else:
                object_node = None

            if 'subject_category' in self.edge_filters:
                f = self.edge_filters['subject_category']
                if subject_node:
                    # subject node exists in graph
                    if any(x in subject_node['category'] for x in f):
                        pass_filter = True
                    else:
                        return False
                else:
                    # subject node does not exist in graph
                    return False

            if 'object_category' in self.edge_filters:
                f = self.edge_filters['object_category']
                if object_node:
                    # object node exists in graph
                    if any(x in object_node['category'] for x in f):
                        pass_filter = True
                    else:
                        return False
                else:
                    # object node does not exist in graph
                    return False
        else:
            # no edge filters defined
            pass_filter = True
        return pass_filter

    def load_edge(self, edge: Dict) -> None:
        """
        Load an edge into an instance of BaseGraph

        Parameters
        ----------
        edge : Dict
            An edge

        """
        if self.check_edge_filter(edge):
            edge = Transformer.validate_edge(edge)
            kwargs = PandasTransformer._build_kwargs(edge.copy())
            if 'subject' in kwargs and 'object' in kwargs:
                if 'id' not in kwargs:
                    kwargs['id'] = generate_uuid()
                s = kwargs['subject']
                o = kwargs['object']
                if 'provided_by' in self.graph_metadata and 'provided_by' not in kwargs.keys():
                    kwargs['provided_by'] = self.graph_metadata['provided_by']
                key = generate_edge_key(s, kwargs['predicate'], o)
                self.graph.add_edge(s, o, key, **kwargs)
                self._edge_properties.update(list(kwargs.keys()))
            else:
                log.info("Ignoring edge with either a missing 'subject' or 'object': {}".format(kwargs))
        else:
            log.debug(f"Edge fails edge filters: {edge}")

    def export_nodes(self, filename: str, delimiter: str) -> None:
        """
        Export nodes from an instance of BaseGraph

        Parameters
        ----------
        filename: str
            The filename
        delimiter: str
            The delimiter to use as a separator

        """
        if not self._node_properties:
            self._node_properties = PandasTransformer.get_all_node_properties(self.graph)
        ordered_node_columns = PandasTransformer._order_node_columns(self._node_properties)
        FH = open(filename, 'w')
        FH.write(delimiter.join(ordered_node_columns) + '\n')
        for n, data in self.graph.nodes(data=True):
            row = PandasTransformer._build_export_row(data)
            row['id'] = n
            values = []
            for c in ordered_node_columns:
                if c in row:
                    values.append(str(row[c]))
                else:
                    values.append("")
            FH.write(delimiter.join(values) + '\n')

    def export_edges(self, filename: str, delimiter: str) -> None:
        """
        Export edges from an instance of BaseGraph

        Parameters
        ----------
        filename: str
            The filename
        delimiter: str
            The delimiter to use as a separator

        """
        if not self._edge_properties:
            self._edge_properties = PandasTransformer.get_all_edge_properties(self.graph)
        ordered_edge_columns = PandasTransformer._order_edge_columns(self._edge_properties)
        FH = open(filename, 'w')
        FH.write(delimiter.join(ordered_edge_columns) + '\n')
        for s, o, data in self.graph.edges(data=True):
            data = self.validate_edge(data)
            row = PandasTransformer._build_export_row(data)
            row['subject'] = s
            row['object'] = o
            values = []
            for c in ordered_edge_columns:
                if c in row:
                    values.append(str(row[c]))
                else:
                    values.append("")
            FH.write(delimiter.join(values) + '\n')

    def save(self, filename: str, output_format: str = 'tsv', compression: Optional[str] = None, **kwargs: Dict) -> str:
        """
        Writes two files representing the node set and edge set
        of an instance of BaseGraph and add them to a `.tar` archive.

        ..note::
            If your node/edge properties are likely to contain commas then it is recommended
            to export to a TSV format instead of CSV.

        Parameters
        ----------
        filename: str
            Name of tar archive file to create
        output_format: str
            The output file format (``tsv``, by default)
        compression: Optional[str]
            The compression. For example, `tar`
        kwargs: Dict
            Any additional arguments

        Returns
        -------
        str
            The filename

        """
        if output_format not in _extension_types:
            raise Exception('Unsupported output format: ' + output_format)
        else:
            delimiter = _extension_types[output_format]
            dirname = os.path.abspath(os.path.dirname(filename))
            basename = os.path.basename(filename)
            extension = output_format.split(':')[0]
            mode = _archive_write_mode[compression] if compression in _archive_write_mode else None
            nodes_file_basename = f"{basename}_nodes.{extension}"
            edges_file_basename = f"{basename}_edges.{extension}"
            if dirname:
                os.makedirs(dirname, exist_ok=True)

            nodes_file_name = os.path.join(dirname if dirname else '', nodes_file_basename)
            edges_file_name = os.path.join(dirname if dirname else '', edges_file_basename)

            if output_format in {'csv:neo4j', 'tsv:neo4j'}:
                self.export_neo4j_nodes(nodes_file_name, delimiter)
                self.export_neo4j_edges(edges_file_name, delimiter)
            else:
                self.export_nodes(nodes_file_name, delimiter)
                self.export_edges(edges_file_name, delimiter)

            if mode:
                archive_basename = f"{basename}.{_archive_format[mode]}"
                archive_name = os.path.join(dirname if dirname else '', archive_basename)
                with tarfile.open(name=archive_name, mode=mode) as tar:
                    tar.add(nodes_file_name, arcname=nodes_file_basename)
                    tar.add(edges_file_name, arcname=edges_file_basename)
                    if os.path.isfile(nodes_file_name):
                        os.remove(nodes_file_name)
                    if os.path.isfile(edges_file_name):
                        os.remove(edges_file_name)

        return filename

    def export_neo4j_nodes(self, filename: str, delimiter: str) -> None:
        """
        Export nodes from an instance of BaseGraph in Neo4j compatible format.
        This format is meant for use with the ``neo4j-admin import`` tool.

        Parameters
        ----------
        filename: str
            The filename
        delimiter: str
            The delimiter to use as a separator

        """
        if not self._node_properties:
            self._node_properties = PandasTransformer.get_all_node_properties(self.graph)
        ordered_node_columns = PandasTransformer._order_node_columns(self._node_properties)
        header = []
        for x in ordered_node_columns:
            if x == 'id':
                header.append(f"{x}:ID")
            elif x == 'category':
                header.append(f"{x}:LABEL")
            elif x in _column_types and _column_types[x] == list:
                header.append(f"{x}:string[]")
            else:
                header.append(x)

        FH = open(filename, 'w')
        FH.write(delimiter.join(header) + '\n')
        for n, data in self.graph.nodes(data=True):
            row = PandasTransformer._build_export_row(data)
            row['id'] = n
            values = []
            for c in ordered_node_columns:
                if c in row:
                    values.append(row[c])
                else:
                    values.append("")
            FH.write(delimiter.join(values) + '\n')

    def export_neo4j_edges(self, filename: str, delimiter: str) -> None:
        """
        Export edges from an instance of BaseGraph in Neo4j compatible format.
        This format is meant for use with the ``neo4j-admin import`` tool.

        Parameters
        ----------
        filename: str
            The filename
        delimiter: str
            The delimiter to use as a separator

        """
        if not self._edge_properties:
            self._edge_properties = PandasTransformer.get_all_edge_properties(self.graph)
        ordered_edge_columns = PandasTransformer._order_edge_columns(self._edge_properties)
        header = []
        for x in ordered_edge_columns:
            if x == 'subject':
                header.append(f"{x}:START_ID")
            elif x == 'object':
                header.append(f"{x}:END_ID")
            elif x == 'predicate':
                header.append(f"{x}:TYPE")
            elif x in _column_types and _column_types[x] == list:
                header.append(f"{x}:string[]")
            else:
                header.append(x)

        FH = open(filename, 'w')
        FH.write(delimiter.join(header) + '\n')
        for s, o, data in self.graph.edges(data=True):
            data = self.validate_edge(data)
            row = PandasTransformer._build_export_row(data)
            row['subject'] = s
            row['object'] = o
            values = []
            for c in ordered_edge_columns:
                if c in row:
                    values.append(str(row[c]))
                else:
                    values.append("")
            FH.write(delimiter.join(values) + '\n')

    @staticmethod
    def _build_kwargs(data: Dict) -> Dict:
        """
        Sanitize key-value pairs in dictionary.

        Parameters
        ----------
        data: Dict
            A dictionary containing key-value pairs

        Returns
        -------
        Dict
            A dictionary containing processed key-value pairs

        """
        tidy_data = {}
        for key, value in data.items():
            new_value = PandasTransformer._remove_null(value)
            if new_value:
                tidy_data[key] = PandasTransformer._sanitize_import(key, new_value)
        return tidy_data

    @staticmethod
    def _build_export_row(data: Dict) -> Dict:
        """
        Casts all values to primitive types like str or bool according to the
        specified type in ``_column_types``. Lists become pipe delimited strings.

        Parameters
        ----------
        data: Dict
            A dictionary containing key-value pairs

        Returns
        -------
        Dict
            A dictionary containing processed key-value pairs

        """
        tidy_data = {}
        for key, value in data.items():
            new_value = PandasTransformer._remove_null(value)
            if new_value:
                tidy_data[key] = PandasTransformer._sanitize_export(key, new_value)
        return tidy_data

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
        core_columns = OrderedSet(['id', 'category', 'name', 'description', 'xref', 'provided_by', 'synonym'])
        ordered_columns = OrderedSet()
        for c in core_columns:
            if c in node_columns:
                ordered_columns.add(c)
                node_columns.remove(c)
        internal_columns = set()
        remaining_columns = node_columns.copy()
        for c in node_columns:
            if c.startswith('_'):
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
        core_columns = OrderedSet(['id', 'subject', 'predicate', 'object', 'category', 'relation', 'provided_by'])
        ordered_columns = OrderedSet()
        for c in core_columns:
            if c in edge_columns:
                ordered_columns.add(c)
                edge_columns.remove(c)
        internal_columns = set()
        remaining_columns = edge_columns.copy()
        for c in edge_columns:
            if c.startswith('_'):
                internal_columns.add(c)
                remaining_columns.remove(c)
        ordered_columns.update(sorted(remaining_columns))
        ordered_columns.update(sorted(internal_columns))
        return ordered_columns

    @staticmethod
    def get_all_node_properties(graph: BaseGraph) -> Set:
        """
        Given a graph, get all possible property names for nodes.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            A graph

        Returns
        -------
        Set
            A set of node properties

        """
        properties = set()
        for n, data in graph.nodes(data=True):
            properties.update(list(data.keys()))

        return properties

    @staticmethod
    def get_all_edge_properties(graph: BaseGraph) -> Set:
        """
        Given a graph, get all possible property names for edges.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            A graph

        Returns
        -------
        Set
            A set of edge properties

        """
        properties = set()
        for u, v, k, data in graph.edges(keys=True, data=True):
            properties.update(list(data.keys()))
        return properties

    @staticmethod
    def _sanitize_export(key: str, value: Any) -> Any:
        """
        Sanitize value for a key for the purpose of export.

        Parameters
        ----------
        key: str
            Key corresponding to a node/edge property
        value: Any
            Value corresponding to the key

        Returns
        -------
        value: Any
            Sanitized value

        """
        new_value: Any
        if key in _column_types:
            if _column_types[key] == list:
                if isinstance(value, (list, set, tuple)):
                    value = [v.replace('\n', ' ').replace('\\"', '').replace('\t', ' ') if isinstance(v, str) else v for v in value]
                    new_value = LIST_DELIMITER.join([str(x) for x in value])
                else:
                    new_value = str(value).replace('\n', ' ').replace('\\"', '').replace('\t', ' ')
            elif _column_types[key] == bool:
                try:
                    new_value = bool(value)
                except:
                    new_value = False
            else:
                new_value = str(value).replace('\n', ' ').replace('\\"', '').replace('\t', ' ')
        else:
            if type(value) == list:
                new_value = LIST_DELIMITER.join([str(x) for x in value])
                new_value = new_value.replace('\n', ' ').replace('\\"', '').replace('\t', ' ')
                _column_types[key] = list
            elif type(value) == bool:
                try:
                    new_value = bool(value)
                    _column_types[key] = bool
                except:
                    new_value = False
            else:
                new_value = str(value).replace('\n', ' ').replace('\\"', '').replace('\t', ' ')
        return new_value

    @staticmethod
    def _sanitize_import(key: str, value: Any) -> Any:
        """
        Sanitize value for a key for the purpose of import.

        Parameters
        ----------
        key: str
            Key corresponding to a node/edge property
        value: Any
            Value corresponding to the key

        Returns
        -------
        value: Any
            Sanitized value

        """
        new_value: Any
        if key in _column_types:
            if _column_types[key] == list:
                if isinstance(value, (list, set, tuple)):
                    value = [v.replace('\n', ' ').replace('\t', ' ') if isinstance(v, str) else v for v in value]
                    new_value = list(value)
                elif isinstance(value, str):
                    value = value.replace('\n', ' ').replace('\t', ' ')
                    new_value = [x for x in value.split(LIST_DELIMITER) if x]
                else:
                    new_value = [str(value).replace('\n', ' ').replace('\t', ' ')]
            elif _column_types[key] == bool:
                try:
                    new_value = bool(value)
                except:
                    new_value = False
            else:
                new_value = str(value).replace('\n', ' ').replace('\t', ' ')
        else:
            if isinstance(value, (list, set, tuple)):
                value = [v.replace('\n', ' ').replace('\t', ' ') if isinstance(v, str) else v for v in value]
                new_value = list(value)
            elif isinstance(value, str):
                if LIST_DELIMITER in value:
                    value = value.replace('\n', ' ').replace('\t', ' ')
                    new_value = [x for x in value.split(LIST_DELIMITER) if x]
                else:
                    new_value = value.replace('\n', ' ').replace('\t', ' ')
            elif isinstance(value, bool):
                try:
                    new_value = bool(value)
                except:
                    new_value = False
            else:
                new_value = str(value).replace('\n', ' ').replace('\t', ' ')
        return new_value

    @staticmethod
    def _remove_null(input: Any) -> Any:
        """
        Remove any null values from input.

        Parameters
        ----------
        input: Any
            Can be a str, list or dict

        Returns
        -------
        Any
            The input without any null values

        """
        new_value: Any = None
        if isinstance(input, (list, set, tuple)):
            # value is a list, set or a tuple
            new_value = []
            for v in input:
                x = PandasTransformer._remove_null(v)
                if x:
                    new_value.append(x)
        elif isinstance(input, dict):
            # value is a dict
            new_value = {}
            for k, v in input.items():
                x = PandasTransformer._remove_null(v)
                if x:
                    new_value[k] = x
        elif isinstance(input, str):
            # value is a str
            if not PandasTransformer.is_null(input):
                new_value = input
        else:
            if not PandasTransformer.is_null(input):
                new_value = input
        return new_value

    @staticmethod
    def is_null(item: Any) -> bool:
        """
        Checks if a given item is null or correspond to null.

        This method checks for: None, numpy.nan, pandas.NA,
        pandas.NaT, "", and " "

        Parameters
        ----------
        item: Any
            The item to check

        Returns
        -------
        bool
            Whether the given item is null or not

        """
        null_values = {np.nan, pd.NA, pd.NaT, None, "", " "}
        return item in null_values
