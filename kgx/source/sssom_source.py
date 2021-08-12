"""
KGX Source for Simple Standard for Sharing Ontology Mappings ("SSSOM")
"""
import gzip
import re

import pandas as pd
from typing import Optional, Generator, Any, Dict, Tuple

import yaml

from kgx.prefix_manager import PrefixManager
from kgx.config import get_logger
from kgx.source import Source
from kgx.utils.kgx_utils import (
    validate_node,
    sanitize_import,
    validate_edge,
    generate_uuid,
    generate_edge_key,
)
from kgx.utils.rdf_utils import process_predicate

log = get_logger()

SSSOM_NODE_PROPERTY_MAPPING = {
    "subject_id": "id",
    "subject_category": "category",
    "object_id": "id",
    "object_category": "category",
}


class SssomSource(Source):
    """
    SssomSource is responsible for reading data as records
    from an SSSOM file.
    """

    def __init__(self):
        super().__init__()
        self.predicate_mapping = {}

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
    ) -> Generator:
        """
        Parse a SSSOM TSV

        Parameters
        ----------
        filename: str
            File to read from
        format: str
            The input file format (``tsv``, by default)
        compression: Optional[str]
            The compression (``gz``)
        kwargs: Dict
            Any additional arguments

        Returns
        -------
        Generator
            A generator for node and edge records

        """
        if "delimiter" not in kwargs:
            kwargs["delimiter"] = "\t"
        self.parse_header(filename, compression)

        # SSSOM 'mapping provider' may override the default 'knowledge_source' setting?
        if "mapping_provider" in self.graph_metadata:
            kwargs["knowledge_source"] = self.graph_metadata["mapping_provider"]

        self.set_provenance_map(kwargs)

        if compression:
            FH = gzip.open(filename, "rb")
        else:
            FH = open(filename)
        file_iter = pd.read_csv(
            FH,
            comment="#",
            dtype=str,
            chunksize=10000,
            low_memory=False,
            keep_default_na=False,
            **kwargs,
        )
        for chunk in file_iter:
            yield from self.load_edges(chunk)

    def parse_header(self, filename: str, compression: Optional[str] = None) -> None:
        """
        Parse metadata from SSSOM headers.

        Parameters
        ----------
        filename: str
            Filename to parse
        compression: Optional[str]
            Compression type

        """
        yamlstr = ""
        if compression:
            FH = gzip.open(filename, "rb")
        else:
            FH = open(filename)
        for line in FH:
            if line.startswith("#"):
                yamlstr += re.sub("^#", "", line)
            else:
                break
        if yamlstr:
            metadata = yaml.safe_load(yamlstr)
            log.info(f"Metadata: {metadata}")
            if "curie_map" in metadata:
                self.prefix_manager.update_prefix_map(metadata["curie_map"])
            for k, v in metadata.items():
                self.graph_metadata[k] = v

    def load_node(self, node: Dict) -> Tuple[str, Dict]:
        """
        Load a node into an instance of BaseGraph

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
        if "id" in node_data:
            n = node_data["id"]

            self.set_node_provenance(node_data)

            self.node_properties.update(list(node_data.keys()))
            return n, node_data
        else:
            log.info("Ignoring node with no 'id': {}".format(node))

    def load_edges(self, df: pd.DataFrame) -> Generator:
        """
        Load edges from pandas.DataFrame into an instance of BaseGraph

        Parameters
        ----------
        df : pandas.DataFrame
            Dataframe containing records that represent edges

        Returns
        -------
        Generator
            A generator for edge records

        """
        for obj in df.to_dict("records"):
            yield from self.load_edge(obj)

    def load_edge(self, edge: Dict) -> Generator:
        """
        Load an edge into an instance of BaseGraph

        Parameters
        ----------
        edge : Dict
            An edge

        Returns
        -------
        Generator
            A generator for node and edge records

        """
        (element_uri, canonical_uri, predicate, property_name) = process_predicate(
            self.prefix_manager, edge["predicate_id"], self.predicate_mapping
        )
        if element_uri:
            edge_predicate = element_uri
        elif predicate:
            edge_predicate = predicate
        else:
            edge_predicate = property_name
        if canonical_uri:
            edge_predicate = element_uri
        data = {
            "subject": edge["subject_id"],
            "predicate": edge_predicate,
            "object": edge["object_id"],
        }
        del edge["predicate_id"]
        data = validate_edge(data)
        subject_node = {}
        object_node = {}
        for k, v in edge.items():
            if k in SSSOM_NODE_PROPERTY_MAPPING:
                if k.startswith("subject"):
                    mapped_k = SSSOM_NODE_PROPERTY_MAPPING[k]
                    if mapped_k == "category" and not PrefixManager.is_curie(v):
                        v = f"biolink:OntologyClass"
                    subject_node[mapped_k] = v
                elif k.startswith("object"):
                    mapped_k = SSSOM_NODE_PROPERTY_MAPPING[k]
                    if mapped_k == "category" and not PrefixManager.is_curie(v):
                        v = f"biolink:OntologyClass"
                    object_node[mapped_k] = v
                else:
                    log.info(f"Ignoring {k} {v}")
            else:
                data[k] = v

        objs = [self.load_node(subject_node), self.load_node(object_node)]

        for k, v in self.graph_metadata.items():
            if k not in {"curie_map"}:
                data[k] = v

        edge_data = sanitize_import(data.copy())
        if "subject" in edge_data and "object" in edge_data:
            if "id" not in edge_data:
                edge_data["id"] = generate_uuid()
            s = edge_data["subject"]
            o = edge_data["object"]

            self.set_edge_provenance(edge_data)

            key = generate_edge_key(s, edge_data["predicate"], o)
            self.edge_properties.update(list(edge_data.keys()))
            objs.append((s, o, key, edge_data))
        else:
            log.info(
                "Ignoring edge with either a missing 'subject' or 'object': {}".format(
                    edge_data
                )
            )

        for o in objs:
            yield o
