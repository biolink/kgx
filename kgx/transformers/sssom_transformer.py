import gzip
from typing import Optional, Dict, Set
import pandas as pd
from bmt import Toolkit
from rdflib import URIRef

from kgx import Transformer, PandasTransformer, PrefixManager
from kgx.config import get_logger
from kgx.graph.base_graph import BaseGraph
from kgx.transformers.rdf_graph_mixin import RdfGraphMixin
from kgx.utils.kgx_utils import generate_uuid, generate_edge_key

log = get_logger()


SSSOM_NODE_PROPERTY_MAPPING = {
    'subject_id': 'id',
    'subject_label': 'name',
    'subject_category': 'category',
    'subject_source': 'source',
    'subject_source_version': 'source_version',
    'object_id': 'id',
    'object_label': 'name',
    'object_category': 'category',
    'object_source': 'source',
    'object_source_version': 'source_version',
}


class SssomTransformer(RdfGraphMixin, Transformer):
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
        self.prefix_manager = PrefixManager()
        self.toolkit = Toolkit()
        self._node_properties: Set = set()
        self._edge_properties: Set = set()

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
        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]
        if compression:
            FH = gzip.open(filename, 'rb')
        else:
            FH = open(filename)
        file_iter = pd.read_csv(FH, dtype=str, chunksize=10000, low_memory=False, keep_default_na=False, **kwargs)
        for chunk in file_iter:
            self.load_edges(chunk)

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
            log.info(obj)
            self.load_edge(obj)

    def load_edge(self, edge: Dict) -> None:
        """
        Load an edge into an instance of BaseGraph

        Parameters
        ----------
        edge : Dict
            An edge

        """
        (element_uri, canonical_uri, predicate, property_name) = self.process_predicate(edge['predicate_id'])
        if element_uri:
            edge_predicate = element_uri
        elif predicate:
            edge_predicate = predicate
        else:
            edge_predicate = property_name
        if canonical_uri:
            edge_predicate = element_uri
        log.info(f"Predicate {edge['predicate_id']} mapped to {edge_predicate}")
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
                    subject_node[SSSOM_NODE_PROPERTY_MAPPING[k]] = v
                elif k.startswith('object'):
                    object_node[SSSOM_NODE_PROPERTY_MAPPING[k]] = v
                else:
                    log.info(f"Ignoring {k} {v}")
            else:
                data[k] = v

        self.load_node(subject_node)
        self.load_node(object_node)

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
