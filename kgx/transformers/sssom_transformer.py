import gzip
import re
from typing import Optional, Dict, Set
import pandas as pd
import yaml
from rdflib import URIRef

from kgx import Transformer, PandasTransformer, PrefixManager
from kgx.config import get_logger
from kgx.graph.base_graph import BaseGraph
from kgx.transformers.rdf_graph_mixin import RdfGraphMixin
from kgx.utils.kgx_utils import generate_uuid, generate_edge_key
from kgx.utils.rdf_utils import process_predicate

log = get_logger()


SSSOM_NODE_PROPERTY_MAPPING = {
    'subject_id': 'id',
    'subject_category': 'category',
    'object_id': 'id',
    'object_category': 'category'
}


class SssomTransformer(Transformer):
    """
    Transformer that parses a SSSOM TSV and loads nodes and edges
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
        self.prefix_manager = PrefixManager()
        self.predicate_mapping = {}
        self.reverse_predicate_mapping = {}

    def set_predicate_mapping(self, m: Dict) -> None:
        """
        Set predicate mappings.

        Use this method to update predicate mappings for predicates that are
        not in Biolink Model

        Parameters
        ----------
        m: Dict
            A dictionary where the keys are IRIs and values are their corresponding property names

        """
        for k, v in m.items():
            if self.prefix_manager.is_curie(k):
                p = self.prefix_manager.expand(k)
            else:
                p = k
            self.predicate_mapping[URIRef(p)] = v
            self.reverse_predicate_mapping[v] = URIRef(p)

    def parse(self, filename: str, input_format: str = 'tsv', compression: str = None, provided_by: Optional[str] = None, **kwargs) -> None:
        """
        Parse a SSSOM TSV

        Parameters
        ----------
        filename: str
            File to read from
        input_format: str
            The input file format (``tsv``, by default)
        compression: Optional[str]
            The compression (``gz``)
        provided_by: Optional[str]
            Define the source providing the input file
        kwargs: Dict
            Any additional arguments

        """
        if 'delimiter' not in kwargs:
            kwargs['delimiter'] = '\t'
        self.parse_header(filename, compression)
        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]
        if 'mapping_provider' in self.graph_metadata:
            self.graph_metadata['provided_by'] = self.graph_metadata['mapping_provider']
        if compression:
            FH = gzip.open(filename, 'rb')
        else:
            FH = open(filename)
        file_iter = pd.read_csv(FH, comment='#', dtype=str, chunksize=10000, low_memory=False, keep_default_na=False, **kwargs)
        for chunk in file_iter:
            self.load_edges(chunk)

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
            FH = gzip.open(filename, 'rb')
        else:
            FH = open(filename)
        for line in FH:
            if line.startswith('#'):
                yamlstr += re.sub('^#', '', line)
            else:
                break
        if yamlstr:
            metadata = yaml.safe_load(yamlstr)
            log.info(f"Metadata: {metadata}")
            if 'curie_map' in metadata:
                self.prefix_manager.update_prefix_map(metadata['curie_map'])
            for k, v in metadata.items():
                self.graph_metadata[k] = v

    def load_node(self, node: Dict) -> None:
        """
        Load a node into an instance of BaseGraph

        Parameters
        ----------
        node : Dict
            A node

        """
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

    def load_edge(self, edge: Dict) -> None:
        """
        Load an edge into an instance of BaseGraph

        Parameters
        ----------
        edge : Dict
            An edge

        """
        (element_uri, canonical_uri, predicate, property_name) = process_predicate(self.prefix_manager, edge['predicate_id'], self.predicate_mapping)
        if element_uri:
            edge_predicate = element_uri
        elif predicate:
            edge_predicate = predicate
        else:
            edge_predicate = property_name
        if canonical_uri:
            edge_predicate = element_uri
        data = {
            'subject': edge['subject_id'],
            'predicate': edge_predicate,
            'object': edge['object_id']
        }
        del edge['predicate_id']
        data = Transformer.validate_edge(data)
        subject_node = {}
        object_node = {}
        for k, v in edge.items():
            if k in SSSOM_NODE_PROPERTY_MAPPING:
                if k.startswith('subject'):
                    mapped_k = SSSOM_NODE_PROPERTY_MAPPING[k]
                    if mapped_k == 'category' and not PrefixManager.is_curie(v):
                        v = f"biolink:OntologyClass"
                    subject_node[mapped_k] = v
                elif k.startswith('object'):
                    mapped_k = SSSOM_NODE_PROPERTY_MAPPING[k]
                    if mapped_k == 'category' and not PrefixManager.is_curie(v):
                        v = f"biolink:OntologyClass"
                    object_node[mapped_k] = v
                else:
                    log.info(f"Ignoring {k} {v}")
            else:
                data[k] = v

        self.load_node(subject_node)
        self.load_node(object_node)

        for k, v in self.graph_metadata.items():
            if k not in {'curie_map'}:
                data[k] = v

        kwargs = PandasTransformer._build_kwargs(data.copy())
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

    def save(self, filename: str, output_format: str = 'tsv', compression: Optional[str] = None, **kwargs: Dict) -> str:
        raise NotImplementedError("Writing SSOM not yet implemented.")
