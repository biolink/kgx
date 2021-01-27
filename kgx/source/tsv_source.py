import re
import tarfile
from typing import Dict, Tuple, Iterator, Any, Generator, Optional
import pandas as pd

from kgx.config import get_logger
from kgx.source.source import Source
from kgx.utils.kgx_utils import generate_uuid, generate_edge_key, extension_types, \
    archive_read_mode, column_types, remove_null, sanitize_import

log = get_logger()


class TsvSource(Source):
    """
    TsvSource is responsible for reading data as records
    from a TSV/CSV.
    """

    def __init__(self):
        super().__init__()

    def parse(self, filename: str, format: str, compression: Optional[str] = None, provided_by: str = None, **kwargs: Any) -> Generator:
        """
        This method reads from a TSV/CSV and yields records.

        Parameters
        ----------
        filename: str
            The filename to parse
        format: str
            The format (``tsv``, ``csv``)
        compression: Optional[str]
            The compression type (``gz``, ``tar``, ``tar.gz``)
        provided_by: Optional[str]
            The name of the source providing the input file
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for records

        """
        if 'delimiter' not in kwargs:
            # infer delimiter from file format
            kwargs['delimiter'] = extension_types[format] # type: ignore
        if 'lineterminator' not in kwargs:
            # set '\n' to be the default line terminator to prevent
            # truncation of lines due to hidden/escaped carriage returns
            kwargs['lineterminator'] = '\n' # type: ignore

        mode = archive_read_mode[compression] if compression in archive_read_mode else None

        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]
        if format == 'tsv':
            kwargs['quoting'] = 3 # type: ignore
        if mode:
            with tarfile.open(filename, mode=mode) as tar:
                for member in tar.getmembers():
                    f = tar.extractfile(member)
                    file_iter = pd.read_csv(f, dtype=str, chunksize=10000, low_memory=False, keep_default_na=False, **kwargs)
                    if re.search(f'nodes.{format}', member.name):
                        for chunk in file_iter:
                            self._node_properties.update(chunk.columns)
                            for obj in chunk.to_dict('records'):
                                yield self.load_node(obj)
                    elif re.search(f'edges.{format}', member.name):
                        for chunk in file_iter:
                            self._edge_properties.update(chunk.columns)
                            for obj in chunk.to_dict('records'):
                                yield self.load_edge(obj)
                    else:
                        raise Exception(f'Tar archive contains an unrecognized file: {member.name}')
        else:
            file_iter = pd.read_csv(filename, dtype=str, chunksize=10000, low_memory=False, keep_default_na=False, **kwargs)
            if re.search(f'nodes.{format}', filename):
                for chunk in file_iter:
                    self._node_properties.update(chunk.columns)
                    yield from self.read_nodes(chunk)
            elif re.search(f'edges.{format}', filename):
                for chunk in file_iter:
                    self._edge_properties.update(chunk.columns)
                    yield from self.read_edges(chunk)
            else:
                raise Exception(f'Unrecognized file: {filename}')

    def read_nodes(self, df: pd.DataFrame) -> Generator:
        """
        Read records from pandas.DataFrame and yield records.

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe containing records that represent nodes

        Returns
        -------
        Generator
            A generator for node records

        """
        for obj in df.to_dict('records'):
            yield self.read_node(obj)

    def read_node(self, node: Dict) -> Tuple[str, Dict]:
        """
        Prepare a node.

        Parameters
        ----------
        node: Dict
            A node

        """
        node = self.validate_node(node)
        kwargs = TsvSource._build_kwargs(node.copy())
        if 'id' in kwargs:
            n = kwargs['id']
            if 'provided_by' in self.graph_metadata and 'provided_by' not in kwargs.keys():
                kwargs['provided_by'] = self.graph_metadata['provided_by']
            self._node_properties.update(list(kwargs.keys()))
            return n, kwargs
        else:
            log.info(f"Ignoring node with no 'id': {node}")

    def read_edges(self, df: pd.DataFrame) -> Generator:
        """
        Load edges from pandas.DataFrame into an instance of BaseGraph

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe containing records that represent edges

        Returns
        -------
        Generator
            A generator for edge records

        """
        for obj in df.to_dict('records'):
            yield self.read_edge(obj)

    def read_edge(self, edge: Dict) -> Tuple[str, str, str, Dict]:
        """
        Load an edge into an instance of BaseGraph.

        Parameters
        ----------
        edge: Dict
            An edge

        """
        edge = self.validate_edge(edge)
        kwargs = TsvSource._build_kwargs(edge.copy())
        if 'id' not in kwargs:
            kwargs['id'] = generate_uuid()
        s = kwargs['subject']
        o = kwargs['object']
        if 'provided_by' in self.graph_metadata and 'provided_by' not in kwargs.keys():
            kwargs['provided_by'] = self.graph_metadata['provided_by']
        key = generate_edge_key(s, kwargs['predicate'], o)
        self._edge_properties.update(list(kwargs.keys()))
        return s, o, key, kwargs

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
            new_value = remove_null(value)
            if new_value:
                tidy_data[key] = sanitize_import(key, new_value)
        return tidy_data

