import re
import tarfile
from typing import Dict, Tuple, Any, Generator, Optional, Union
import pandas as pd

from kgx.config import get_logger
from kgx.source.source import Source
from kgx.utils.kgx_utils import (
    generate_uuid,
    generate_edge_key,
    extension_types,
    archive_read_mode,
    remove_null,
    sanitize_import,
    validate_edge,
    validate_node,
)

log = get_logger()


class TsvSource(Source):
    """
    TsvSource is responsible for reading data as records
    from a TSV/CSV.
    """

    def __init__(self):
        super().__init__()

    def set_prefix_map(self, m: Dict) -> None:
        """
        Add or override default prefix to IRI map.

        Parameters
        ----------
        m: Dict
            Prefix to IRI map

        """
        self.prefix_manager.set_prefix_map(m)

    def set_reverse_prefix_map(self, m: Dict) -> None:
        """
        Add or override default IRI to prefix map.

        Parameters
        ----------
        m: Dict
            IRI to prefix map

        """
        self.prefix_manager.set_reverse_prefix_map(m)

    def parse(
        self,
        filename: str,
        format: str,
        compression: Optional[str] = None,
        provided_by: str = None,
        **kwargs: Any,
    ) -> Generator:
        """
        This method reads from a TSV/CSV and yields records.

        Parameters
        ----------
        filename: str
            The filename to parse
        format: str
            The format (``tsv``, ``csv``)
        compression: Optional[str]
            The compression type (``tar``, ``tar.gz``)
        provided_by: Optional[str]
            The name of the source providing the input file
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for node and edge records

        """
        if 'delimiter' not in kwargs:
            # infer delimiter from file format
            kwargs['delimiter'] = extension_types[format]  # type: ignore
        if 'lineterminator' not in kwargs:
            # set '\n' to be the default line terminator to prevent
            # truncation of lines due to hidden/escaped carriage returns
            kwargs['lineterminator'] = '\n'  # type: ignore

        mode = archive_read_mode[compression] if compression in archive_read_mode else None

        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]
        if format == 'tsv':
            kwargs['quoting'] = 3  # type: ignore
        if mode:
            with tarfile.open(filename, mode=mode) as tar:
                for member in tar.getmembers():
                    f = tar.extractfile(member)
                    file_iter = pd.read_csv(
                        f,
                        dtype=str,
                        chunksize=10000,
                        low_memory=False,
                        keep_default_na=False,
                        **kwargs,
                    )
                    if re.search(f'nodes.{format}', member.name):
                        for chunk in file_iter:
                            self.node_properties.update(chunk.columns)
                            yield from self.read_nodes(chunk)
                    elif re.search(f'edges.{format}', member.name):
                        for chunk in file_iter:
                            self.edge_properties.update(chunk.columns)
                            yield from self.read_edges(chunk)
                    else:
                        raise Exception(f'Tar archive contains an unrecognized file: {member.name}')
        else:
            file_iter = pd.read_csv(
                filename,
                dtype=str,
                chunksize=10000,
                low_memory=False,
                keep_default_na=False,
                **kwargs,
            )
            if re.search(f'nodes.{format}', filename):
                for chunk in file_iter:
                    self.node_properties.update(chunk.columns)
                    yield from self.read_nodes(chunk)
            elif re.search(f'edges.{format}', filename):
                for chunk in file_iter:
                    self.edge_properties.update(chunk.columns)
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

    def read_node(self, node: Dict) -> Optional[Tuple[str, Dict]]:
        """
        Prepare a node.

        Parameters
        ----------
        node: Dict
            A node

        Returns
        -------
        Optional[Tuple[str, Dict]]
            A tuple that contains node id and node data

        """
        node = validate_node(node)
        node_data = sanitize_import(node.copy())
        if 'id' in node_data:
            n = node_data['id']
            if 'provided_by' in self.graph_metadata and 'provided_by' not in node_data.keys():
                node_data['provided_by'] = self.graph_metadata['provided_by']
            self.node_properties.update(list(node_data.keys()))
            if self.check_node_filter(node_data):
                self.node_properties.update(node_data.keys())
                return n, node_data
        else:
            log.info(f"Ignoring node with no 'id': {node}")

    def read_edges(self, df: pd.DataFrame) -> Generator:
        """
        Load edges from pandas.DataFrame into an instance of BaseGraph.

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

    def read_edge(self, edge: Dict) -> Optional[Tuple]:
        """
        Load an edge into an instance of BaseGraph.

        Parameters
        ----------
        edge: Dict
            An edge

        Returns
        -------
        Optional[Tuple]
            A tuple that contains subject id, object id, edge key, and edge data

        """
        edge = validate_edge(edge)
        edge_data = sanitize_import(edge.copy())
        if 'id' not in edge_data:
            edge_data['id'] = generate_uuid()
        s = edge_data['subject']
        o = edge_data['object']
        if 'provided_by' in self.graph_metadata and 'provided_by' not in edge_data.keys():
            edge_data['provided_by'] = self.graph_metadata['provided_by']
        key = generate_edge_key(s, edge_data['predicate'], o)
        self.edge_properties.update(list(edge_data.keys()))
        if self.check_edge_filter(edge_data):
            self.node_properties.update(edge_data.keys())
            return s, o, key, edge_data
