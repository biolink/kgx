import logging
import tarfile
from tempfile import TemporaryFile
from typing import List, Dict

import numpy as np
import pandas as pd

from kgx.transformer import Transformer
from kgx.utils import make_path

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

    def parse(self, filename: str, input_format: str = 'csv', **kwargs) -> None:
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
            The input file format ('csv', by default)
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

        if mode:
            with tarfile.open(filename, mode=mode) as tar:
                for member in tar.getmembers():
                    f = tar.extractfile(member)
                    df = pd.read_csv(f, comment='#', **kwargs) # type: pd.DataFrame
                    if member.name == "nodes.{}".format(input_format):
                        self.load_nodes(df)
                    elif member.name == "edges.{}".format(input_format):
                        self.load_edges(df)
                    else:
                        raise Exception('Tar archive contains an unrecognized file: {}'.format(member.name))
        else:
            df = pd.read_csv(filename, comment='#', dtype=str, **kwargs) # type: pd.DataFrame
            self.load(df)

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
            self.graph.add_edge(s, o, **kwargs)
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

    def save(self, filename: str, extension: str = 'csv', mode: str = 'w', **kwargs) -> str:
        """
        Writes two files representing the node set and edge set of a networkx.MultiDiGraph,
        and add them to a .tar archive.

        Parameters
        ----------
        filename: str
            Name of tar archive file to create
        extension: str
            The output file format (csv, by default)
        mode: str
            Form of compression to use ('w', by default, signifies no compression)
        kwargs: dict
            Any additional arguments

        """
        if extension not in _extension_types:
            raise Exception('Unsupported extension: ' + extension)

        archive_name = "{}.{}".format(filename, _archive_format[mode])
        delimiter = _extension_types[extension]

        nodes_content = self.export_nodes().to_csv(sep=delimiter, index=False, escapechar="\\", doublequote=False)
        edges_content = self.export_edges().to_csv(sep=delimiter, index=False, escapechar="\\", doublequote=False)

        nodes_file_name = 'nodes.' + extension
        edges_file_name = 'edges.' + extension

        make_path(archive_name)
        with tarfile.open(name=archive_name, mode=mode) as tar:
            PandasTransformer._add_to_tar(tar, nodes_file_name, nodes_content)
            PandasTransformer._add_to_tar(tar, edges_file_name, edges_content)

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
        # remove numpy.nan
        data = {k : v for k, v in data.items() if v is not np.nan}

        for key, value in data.items():
            # process value as a list if key is a multi-valued property
            if key in _column_types:
                if _column_types[key] == list:
                    if isinstance(value, (list, set, tuple)):
                        data[key] = list(value)
                    elif isinstance(value, str):
                        data[key] = value.split(LIST_DELIMITER)
                    else:
                        data[key] = [str(value)]
                elif _column_types[key] == bool:
                    try:
                        data[key] = bool(value)
                    except:
                        data[key] = False
                else:
                    data[key] = str(value)
        return data

    @staticmethod
    def _build_export_row(data: Dict) -> Dict:
        """
        Casts all values to primitive types like str or bool according to the
        specified type in `_column_types`. Lists become pipe delimited strings.

        Parameters
        ----------
        data: dict
            A dictionary containing key-value pairs

        Returns
        -------
        dict
            A dictionary containing processed key-value pairs

        """
        data = {k : v for k, v in data.items() if v is not np.nan}
        for key, value in data.items():
            if key in _column_types:
                if _column_types[key] == list:
                    if isinstance(value, (list, set, tuple)):
                        data[key] = LIST_DELIMITER.join(value)
                    else:
                        data[key] = str(value)
                elif _column_types[key] == bool:
                    try:
                        data[key] = bool(value)
                    except:
                        data[key] = False
                else:
                    data[key] = str(value)
        return data

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
    def _add_to_tar(tar: tarfile.TarFile, filename: str, filecontent: pd.DataFrame) -> None:
        """
        Write file contents to a given filename and add the file
        to a specified tar archive.

        Parameters
        ----------
        tar: tarfile.TarFile
            Tar archive handle
        filename: str
            Name of file to add to the archive
        filecontent: pandas.DataFrame
            DataFrame containing data to write to filename

        """
        content = filecontent.encode()
        with TemporaryFile() as tmp:
            tmp.write(content)
            tmp.seek(0)
            info = tarfile.TarInfo(name=filename)
            info.size = len(content)
            tar.addfile(tarinfo=info, fileobj=tmp)
