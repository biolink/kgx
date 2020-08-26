from typing import Dict, List

from kgx.config import get_logger
from kgx.transformers.json_transformer import JsonTransformer
from kgx.utils.kgx_utils import generate_edge_key

log = get_logger()


class RsaTransformer(JsonTransformer):
    """
    Transformer that parses a Reasoner Std API format JSON and loads nodes and edges into a networkx.MultiDiGraph
    """
    # TODO: ReasonerStdAPI specification

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
        super().load_node(node)

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
        super().load_edge(edge)
