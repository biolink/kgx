import re
import tarfile
import typing
from typing import Dict, Tuple, Any, Generator, Optional, List
import pandas as pd

from kgx.config import get_logger
from kgx.source.source import Source
from kgx.utils.kgx_utils import (
    generate_uuid,
    generate_edge_key,
    extension_types,
    archive_read_mode,
    sanitize_import
)
log = get_logger()

DEFAULT_LIST_DELIMITER = "|"


class TsvSource(Source):
    """
    TsvSource is responsible for reading data as records
    from a TSV/CSV.
    """

    def __init__(self, owner):
        super().__init__(owner)
        self.list_delimiter = DEFAULT_LIST_DELIMITER

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
        **kwargs: Any,
    ) -> typing.Generator:
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
        kwargs: Any
            Any additional arguments

        Returns
        -------
        Generator
            A generator for node and edge records

        """
        if "delimiter" not in kwargs:
            # infer delimiter from file format
            kwargs["delimiter"] = extension_types[format]
        if "lineterminator" not in kwargs:
            # set '\n' to be the default line terminator to prevent
            # truncation of lines due to hidden/escaped carriage returns
            kwargs["lineterminator"] = "\n"
        if "list_delimeter" in kwargs:
            self.list_delimiter = kwargs["list_delimiter"]

        mode = (
            archive_read_mode[compression] if compression in archive_read_mode else None
        )
        self.set_provenance_map(kwargs)

        if format == "tsv":
            kwargs["quoting"] = 3
        if mode:
            with tarfile.open(filename, mode=mode) as tar:
                # Alas, the order that tar file members is important in some streaming operations
                # (e.g. graph-summary and validation) in that generally, all the node files need to be
                # loaded first,  followed by the  associated edges files can be loaded and analysed.

                # Start by partitioning files of each type into separate lists
                node_files: List[str] = list()
                edge_files: List[str] = list()
                for name in tar.getnames():
                    if re.search(f"nodes.{format}", name):
                        node_files.append(name)
                    elif re.search(f"edges.{format}", name):
                        edge_files.append(name)
                    else:
                        # This used to throw an exception but perhaps we should simply ignore it.
                        log.warning(
                            f"Tar archive contains an unrecognized file: {name}. Skipped..."
                        )

                # Then, first extract and capture contents of the nodes files...
                for name in node_files:
                    try:
                        member = tar.getmember(name)
                    except KeyError:
                        log.warning(
                            f"Node file {name} member in archive {filename} could not be accessed? Skipped?"
                        )
                        continue

                    f = tar.extractfile(member)
                    file_iter = pd.read_csv(
                        f,
                        dtype=str,
                        chunksize=10000,
                        low_memory=False,
                        keep_default_na=False,
                        **kwargs,
                    )
                    for chunk in file_iter:
                        self.node_properties.update(chunk.columns)
                        yield from self.read_nodes(chunk)

                # Next, extract and capture contents of the edges files...
                for name in edge_files:
                    try:
                        member = tar.getmember(name)
                    except KeyError:
                        log.warning(
                            f"Edge file {name} member in archive {filename} could not be accessed? Skipped?"
                        )
                        continue

                    f = tar.extractfile(member)
                    file_iter = pd.read_csv(
                        f,
                        dtype=str,
                        chunksize=10000,
                         low_memory=False,
                        keep_default_na=False,
                        **kwargs,
                    )
                    for chunk in file_iter:
                        self.edge_properties.update(chunk.columns)
                        yield from self.read_edges(chunk)
        else:
            file_iter = pd.read_csv(
                filename,
                dtype=str,
                chunksize=10000,
                low_memory=False,
                keep_default_na=False,
                **kwargs,
            )
            if re.search(f"nodes.{format}", filename):
                for chunk in file_iter:
                    self.node_properties.update(chunk.columns)
                    yield from self.read_nodes(chunk)
            elif re.search(f"edges.{format}", filename):
                for chunk in file_iter:
                    self.edge_properties.update(chunk.columns)
                    yield from self.read_edges(chunk)
            else:
                # This used to throw an exception but perhaps we should simply ignore it.
                log.warning(
                    f"Parse function cannot resolve the KGX file type in name {filename}. Skipped..."
                )

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
        for obj in df.to_dict("records"):
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
        node = self.validate_node(node)
        if node:
            # if not None, assumed to have an "id" here...
            node_data = sanitize_import(node.copy(), self.list_delimiter)
            n = node_data["id"]

            self.set_node_provenance(node_data)  # this method adds provided_by to the node properties/node data
            self.node_properties.update(list(node_data.keys()))
            if self.check_node_filter(node_data):
                self.node_properties.update(node_data.keys())
                return n, node_data

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
        for obj in df.to_dict("records"):
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
        edge = self.validate_edge(edge)
        if not edge:
            return None
        edge_data = sanitize_import(edge.copy(), self.list_delimiter)
        if "id" not in edge_data:
            edge_data["id"] = generate_uuid()
        s = edge_data["subject"]
        o = edge_data["object"]
        self.set_edge_provenance(edge_data)
        key = generate_edge_key(s, edge_data["predicate"], o)
        self.edge_properties.update(list(edge_data.keys()))
        if self.check_edge_filter(edge_data):
            self.edge_properties.update(edge_data.keys())
            return s, o, key, edge_data
