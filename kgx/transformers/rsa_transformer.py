from typing import Dict, List, Any, Optional

from kgx.config import get_logger
from kgx.graph.base_graph import BaseGraph
from kgx.transformers.json_transformer import JsonTransformer

log = get_logger()


class RsaTransformer(JsonTransformer):
    """
    Transformer that parses a Reasoner Std API format JSON
    and loads nodes and edges into an instance of BaseGraph

    Parameters
    ----------
    source_graph: Optional[kgx.graph.base_graph.BaseGraph]
        The source graph

    """
    # TODO: ReasonerStdAPI specification

    def __init__(self, source_graph: Optional[BaseGraph] = None):
        super().__init__(source_graph)

    def load(self, obj: Dict[str, Any]) -> None:
        """
        Load a Reasoner Std API format JSON object, containing nodes and edges,
        into an instance of BaseGraph.

        .. Note::
            Only nodes and edges contained within ``knowledge_graph`` field is loaded.

        Parameters
        ----------
        obj: Dict[str, Any]
            A Reasoner Std API formatted JSON object

        """
        if 'nodes' in obj['knowledge_graph']:
            self.load_nodes(obj['knowledge_graph']['nodes'])
        if 'edges' in obj['knowledge_graph']:
            self.load_edges(obj['knowledge_graph']['edges'])

    def load_node(self, node: Dict) -> None:
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
        super().load_node(node)

    def load_edge(self, edge: Dict) -> None:
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
        super().load_edge(edge)
