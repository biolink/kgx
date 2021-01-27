import gzip
from itertools import chain
from typing import Dict, Tuple

import ijson

from kgx.source.tsv_source import TsvSource


# TODO: update for TRAPI 1.0 spec

class TrapiSource(TsvSource):
    def __init__(self):
        super().__init__()
        self._node_properties = set()
        self._edge_properties = set()

    def parse(self, filename, input_format, compression = None, provided_by = None, **kwargs):
        if provided_by:
            self.graph_metadata['provided_by'] = [provided_by]
        n = self.read_nodes(filename, compression)
        e = self.read_edges(filename, compression)
        return chain(n, e)

    def read_nodes(self, filename, compression):
        if compression == 'gz':
            FH = gzip.open(filename, 'rb')
        else:
            FH = open(filename, 'rb')
        for n in ijson.items(FH, 'knowledge_graph.nodes.item'):
            yield self.load_node(n)

    def read_edges(self, filename, compression):
        if compression == 'gz':
            FH = gzip.open(filename, 'rb')
        else:
            FH = open(filename, 'rb')
        for e in ijson.items(FH, 'knowledge_graph.edges.item'):
            yield self.load_edge(e)

    def load_node(self, node: Dict) -> Tuple[str, Dict]:
        """
        Load a node into an instance of BaseGraph

        .. Note::
            This method transformers Reasoner Std API format fields to Biolink Model fields.

        Parameters
        ----------
        node : Dict
            A node

        """

        if 'type' in node and 'category' not in node:
            node['category'] = node['type']
            del node['type']
        return super().load_node(node)

    def load_edge(self, edge: Dict) -> Tuple[str, str, str, Dict]:
        """
        Load an edge into an instance of BaseGraph

        .. Note::
            This methods transformers Reasoner Std API format fields to Biolink Model fields.

        Parameters
        ----------
        edge : Dict
            An edge

        """
        if 'source_id' in edge:
            edge['subject'] = edge['source_id']
        if 'target_id' in edge:
            edge['object'] = edge['target_id']
        if 'relation_label' in edge:
            edge['predicate'] = edge['relation_label'][0]
        return super().load_edge(edge)
