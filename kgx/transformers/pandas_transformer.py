import os
import re
import pandas as pd
import numpy as np
import logging
import tarfile
from kgx.utils.kgx_utils import generate_edge_key
from kgx.transformers.transformer import Transformer

from typing import List, Dict, Optional, Any

LIST_DELIMITER = '|'

_column_types = {
    'publications': list,
    'qualifiers': list,
    'category': list,
    'synonym': list,
    'provided_by': list,
    'same_as': list,
    'negated': bool,
}

_extension_types = {
    'csv': ',',
    'tsv': '\t',
    'txt': '|'
}

_archive_mode = {
    'tar': 'r',
    'tar.gz': 'r:gz',
    'tar.bz2': 'r:bz2'
}

_archive_format = {
    'w': 'tar',
    'w:gz': 'tar.gz',
    'w:bz2': 'tar.bz2'
}


class PandasTransformer(Transformer):
    """
    Transformer that parses a pandas.DataFrame, and loads nodes and edges into a networkx.MultiDiGraph
    """

    # TODO: Support parsing and export of neo4j-import tool compatible CSVs with appropriate headers

    def parse(self, filename: str, input_format: str = 'csv', provided_by: str = None, **kwargs) -> None:
        """
        Parse a CSV/TSV (or plain text) file.

        The file can represent either nodes (nodes.csv) or edges (edges.csv) or both (data.tar),
        where the tar archive contains nodes.csv and edges.csv

        The file can also be data.tar.gz or data.tar.bz2

        Parameters
        ----------
        filename: str
            File to read from
        input_format: str
            The input file format (``csv``, by default)
        provided_by: str
            Define the source providing the input file
        kwargs: Dict
            Any additional arguments

        """
        if 'delimiter' not in kwargs:
            # infer delimiter from file format
            kwargs['delimiter'] = _extension_types[input_format]

        if filename.endswith('.tar'):
            mode = _archive_mode['tar']
        elif filename.endswith('.tar.gz'):
            mode = _archive_mode['tar.gz']
        elif filename.endswith('.tar.bz2'):
            mode = _archive_mode['tar.bz2']
        else:
            # file is not an archive
            mode = None

        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]

        if mode:
            with tarfile.open(filename, mode=mode) as tar:
                for member in tar.getmembers():
                    f = tar.extractfile(member)
                    iter = pd.read_csv(f, dtype=str, quoting=3, chunksize=10000, low_memory=False, **kwargs) # type: pd.DataFrame
                    if re.search('nodes.{}'.format(input_format), member.name):
                        for chunk in iter:
                            self.load_nodes(chunk)
                    elif re.search('edges.{}'.format(input_format), member.name):
                        for chunk in iter:
                            self.load_edges(chunk)
                    else:
                        raise Exception('Tar archive contains an unrecognized file: {}'.format(member.name))
        else:
            iter = pd.read_csv(filename, dtype=str, quoting=3, chunksize=10000, low_memory=False, **kwargs) # type: pd.DataFrame
            for chunk in iter:
                self.load(chunk)

    def load(self, df: pd.DataFrame) -> None:
        """
        Load a panda.DataFrame, containing either nodes or edges, into a networkx.MultiDiGraph

        Parameters
        ----------
        df : pandas.DataFrame
            Dataframe containing records that represent nodes or edges

        """
        if 'subject' in df:
            self.load_edges(df)
        else:
            self.load_nodes(df)

    def load_nodes(self, df: pd.DataFrame) -> None:
        """
        Load nodes from pandas.DataFrame into a networkx.MultiDiGraph

        Parameters
        ----------
        df : pandas.DataFrame
            Dataframe containing records that represent nodes

        """
        for obj in df.to_dict('record'):
            self.load_node(obj)

    def load_node(self, node: Dict) -> None:
        """
        Load a node into a networkx.MultiDiGraph

        Parameters
        ----------
        node : dict
            A node

        """
        node = Transformer.validate_node(node)
        kwargs = PandasTransformer._build_kwargs(node.copy())
        if 'id' in kwargs:
            n = kwargs['id']
            self.graph.add_node(n, **kwargs)
        else:
            logging.info("Ignoring node with no 'id': {}".format(node))

    def load_edges(self, df: pd.DataFrame) -> None:
        """
        Load edges from pandas.DataFrame into a networkx.MultiDiGraph

        Parameters
        ----------
        df : pandas.DataFrame
            Dataframe containing records that represent edges

        """
        for obj in df.to_dict('record'):
            self.load_edge(obj)

    def load_edge(self, edge: Dict) -> None:
        """
        Load an edge into a networkx.MultiDiGraph

        Parameters
        ----------
        edge : dict
            An edge

        """
        edge = Transformer.validate_edge(edge)
        kwargs = PandasTransformer._build_kwargs(edge.copy())
        if 'subject' in kwargs and 'object' in kwargs:
            s = kwargs['subject']
            o = kwargs['object']
            key = generate_edge_key(s, kwargs['edge_label'], o)
            self.graph.add_edge(s, o, key, **kwargs)
        else:
            logging.info("Ignoring edge with either a missing 'subject' or 'object': {}".format(kwargs))

    def export_nodes(self) -> pd.DataFrame:
        """
        Export nodes from networkx.MultiDiGraph as a pandas.DataFrame

        Returns
        -------
        pandas.DataFrame
            A Dataframe where each record corresponds to a node from the networkx.MultiDiGraph

        """
        rows = []
        for n, data in self.graph.nodes(data=True):
            data = self.validate_node(data)
            row = PandasTransformer._build_export_row(data.copy())
            row['id'] = n
            rows.append(row)
        df = pd.DataFrame.from_records(rows)
        return df

    def export_edges(self) -> pd.DataFrame:
        """
        Export edges from networkx.MultiDiGraph as a pandas.DataFrame

        Returns
        -------
        pandas.DataFrame
            A Dataframe where each record corresponds to an edge from the networkx.MultiDiGraph

        """
        rows = []
        for s, o, data in self.graph.edges(data=True):
            data = self.validate_edge(data)
            row = PandasTransformer._build_export_row(data.copy())
            row['subject'] = s
            row['object'] = o
            rows.append(row)
        df = pd.DataFrame.from_records(rows)
        cols = df.columns.tolist()
        cols = PandasTransformer._order_cols(cols)
        df = df[cols]
        return df

    def save(self, filename: str, extension: str = 'csv', mode: Optional[str] = 'w', **kwargs) -> str:
        """
        Writes two files representing the node set and edge set of a networkx.MultiDiGraph,
        and add them to a `.tar` archive.
        If mode is set to ``None``, then there will be no archive created.

        Parameters
        ----------
        filename: str
            Name of tar archive file to create
        extension: str
            The output file format (``csv``, by default)
        mode: str
            Form of compression to use (``w``, by default, signifies no compression).
        kwargs: dict
            Any additional arguments

        """
        if extension not in _extension_types:
            raise Exception('Unsupported extension: ' + extension)

        delimiter = _extension_types[extension]
        nodes_file_name = "{}_nodes.{}".format(filename, extension)
        edges_file_name = "{}_edges.{}".format(filename, extension)
        file_dir = os.path.dirname(nodes_file_name)
        if file_dir:
            os.makedirs(file_dir, exist_ok=True)

        self.export_nodes().to_csv(sep=delimiter, path_or_buf=nodes_file_name, index=False, escapechar="\\", doublequote=False)
        self.export_edges().to_csv(sep=delimiter, path_or_buf=edges_file_name, index=False, escapechar="\\", doublequote=False)

        if mode:
            archive_name = "{}.{}".format(filename, _archive_format[mode])
            with tarfile.open(name=archive_name, mode=mode) as tar:
                tar.add(nodes_file_name)
                tar.add(edges_file_name)
                if os.path.isfile(nodes_file_name):
                    os.remove(nodes_file_name)
                if os.path.isfile(edges_file_name):
                    os.remove(edges_file_name)

        return filename

    @staticmethod
    def _build_kwargs(data: Dict) -> Dict:
        """
        Sanitize key-value pairs in dictionary.

        Parameters
        ----------
        data: dict
            A dictionary containing key-value pairs

        Returns
        -------
        dict
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
        data: dict
            A dictionary containing key-value pairs

        Returns
        -------
        dict
            A dictionary containing processed key-value pairs

        """
        tidy_data = {}
        for key, value in data.items():
            new_value = PandasTransformer._remove_null(value)
            if new_value:
                tidy_data[key] = PandasTransformer._sanitize_export(key, new_value)
        return tidy_data

    @staticmethod
    def _order_cols(cols: List[str]) -> List[str]:
        """
        Arrange columns in a defined order.

        Parameters
        ----------
        cols: list
            A list with elements in any order

        Returns
        -------
        list
            A list with elements in a particular order

        """
        ORDER = ['id', 'subject', 'predicate', 'object', 'relation']
        cols2 = []
        for c in ORDER:
            if c in cols:
                cols2.append(c)
                cols.remove(c)
        return cols2 + cols

    @staticmethod
    def _sanitize_export(key, value):
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
        if key in _column_types:
            if _column_types[key] == list:
                if isinstance(value, (list, set, tuple)):
                    value = [v.replace('\n', ' ') if isinstance(v, str) else v for v in value]
                    new_value = LIST_DELIMITER.join(value)
                else:
                    new_value = str(value).replace('\n', ' ')
            elif _column_types[key] == bool:
                try:
                    new_value = bool(value)
                except:
                    new_value = False
            else:
                new_value = str(value).replace('\n', ' ')
        else:
            if type(value) == list:
                new_value = LIST_DELIMITER.join(value)
                new_value = new_value.replace('\n', ' ')
            elif type(value) == bool:
                try:
                    new_value = bool(value)
                except:
                    new_value = False
            else:
                new_value = str(value).replace('\n', ' ')
        return new_value

    @staticmethod
    def _sanitize_import(key: str, value: Any):
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
        if key in _column_types:
            if _column_types[key] == list:
                if isinstance(value, (list, set, tuple)):
                    value = [v.replace('\n', ' ') if isinstance(v, str) else v for v in value]
                    new_value = list(value)
                elif isinstance(value, str):
                    value = value.replace('\n', ' ')
                    new_value = value.split(LIST_DELIMITER)
                else:
                    new_value = [str(value).replace('\n', ' ')]
            elif _column_types[key] == bool:
                try:
                    new_value = bool(value)
                except:
                    new_value = False
            else:
                new_value = str(value).replace('\n', ' ')
        else:
            if isinstance(value, (list, set, tuple)):
                value = [v.replace('\n', ' ') if isinstance(v, str) else v for v in value]
                new_value = list(value)
            elif isinstance(value, str):
                if LIST_DELIMITER in value:
                    value = value.replace('\n', ' ')
                    new_value = value.split(LIST_DELIMITER)
                else:
                    new_value = value.replace('\n', ' ')
            elif isinstance(value, bool):
                try:
                    new_value = bool(value)
                except:
                    new_value = False
            else:
                new_value = str(value).replace('\n', ' ')
        return new_value

    @staticmethod
    def _remove_null(input: Any):
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
        new_value = None
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
