import logging
from typing import Dict, List

from kgx import PandasTransformer
from kgx.transformers.json_transformer import JsonTransformer
from kgx.utils.kgx_utils import generate_edge_key


class RsaTransformer(JsonTransformer):
    """
    Transformer that parses a Reasoner Std API format JSON and loads nodes and edges into a networkx.MultiDiGraph
    """

    def load(self, obj: Dict[str, List]) -> None:
        """
        Load a Reasoner Std API format JSON object, containing nodes and edges, into networkx.MultiDiGraph

        .. Note::
            Only nodes and edges contained within ``knowledge_graph`` field is loaded.

        Parameters
        ----------
        obj: dict
            A Reasoner Std API formatted JSON object

        """
        if 'nodes' in obj['knowledge_graph']:
            self.load_nodes(obj['knowledge_graph']['nodes'])
        if 'edges' in obj['knowledge_graph']:
            self.load_edges(obj['knowledge_graph']['edges'])

    def load_node(self, node: dict) -> None:
        """
        Load a node into networkx.MultiDiGraph

        .. Note::
            This method transformers Reasoner Std API format fields to Biolink Model fields.

        Parameters
        ----------
        node : dict
            A node

        """

        if 'type' in node and 'category' not in node:
            node['category'] = node['type']
            del node['type']

        node = self.validate_node(node)
        kwargs = PandasTransformer._build_kwargs(node.copy())
        if 'id' in kwargs:
            n = kwargs['id']
            self.graph.add_node(n, **kwargs)
        else:
            logging.info("Ignoring node with no 'id': {}".format(node))

    def load_edge(self, edge: Dict) -> None:
        """
        Load an edge into a networkx.MultiDiGraph

        .. Note::
            This methods transformers Reasoner Std API format fields to Biolink Model fields.

        Parameters
        ----------
        edge : dict
            An edge

        """
        if 'source_id' in edge:
            edge['subject'] = edge['source_id']
        if 'target_id' in edge:
            edge['object'] = edge['target_id']
        if 'relation_label' in edge:
            edge['edge_label'] = edge['relation_label'][0]

        edge = self.validate_edge(edge)
        kwargs = PandasTransformer._build_kwargs(edge.copy())
        if 'subject' in kwargs and 'object' in kwargs:
            s = kwargs['subject']
            o = kwargs['object']
            key = generate_edge_key(s, kwargs['edge_label'], o)
            self.graph.add_edge(s, o, key, **kwargs)
        else:
            logging.info("Ignoring edge with either a missing 'subject' or 'object': {}".format(kwargs))
