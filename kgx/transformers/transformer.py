import json
from typing import Union, List, Dict, Tuple, Set, Any, Optional

from kgx.config import get_logger, get_config, get_graph_store_class
from kgx.graph.base_graph import BaseGraph
from kgx.graph.nx_graph import NxGraph

log = get_logger()


IGNORE_CLASSES = ['All', 'entity']

ADDITIONAL_LABELS = {
    'phenotypic_abnormality': 'phenotypic_feature',
    'clinical_course': 'phenotypic_feature',
    'blood_group': 'phenotypic_feature',
    'clinical_modifier': 'phenotypic_feature',
    'frequency': 'phenotypic_feature',
    'mode_of_inheritance': 'phenotypic_feature',
    'past_medical_history': 'phenotypic_feature'
}


class Transformer(object):
    """
    Base class for performing a transformation.

    This can be,
     - from a source to an in-memory property graph (kgx.graph.base_graph.BaseGraph)
     - from an in-memory property graph to a target format or database (Neo4j, CSV, RDF Triple Store, TTL)

    Parameters
    ----------
    source_graph: Optional[kgx.graph.base_graph.BaseGraph]
        The source graph

    """

    DEFAULT_NODE_CATEGORY = 'biolink:NamedThing'

    def __init__(self, source_graph: Optional[BaseGraph] = None):
        if source_graph:
            self.graph = source_graph
        else:
            self.graph = get_graph_store_class()()

        self.node_filters: Dict[str, Any] = {}
        self.edge_filters: Dict[str, Any] = {}
        self.graph_metadata: Dict = {}

    def report(self) -> None:
        """
        Print a summary report about self.graph
        """
        log.info('Total nodes in {}: {}'.format(self.graph.name or 'graph', len(self.graph.nodes())))
        log.info('Total edges in {}: {}'.format(self.graph.name or 'graph', len(self.graph.edges())))

    def is_empty(self) -> bool:
        """
        Check whether self.graph is empty.

        Returns
        -------
        bool
            A boolean value asserting whether the graph is empty or not

        """
        return len(self.graph.nodes()) == 0 and len(self.graph.edges()) == 0

    def set_node_filter(self, key: str, value: Union[str, set]) -> None:
        """
        Set a node filter, as defined by a key and value pair.
        These filters are used to create a subgraph or reduce the
        search space when fetching nodes from a source.

        .. note::
            When defining the 'category' filter, the value should be of type ``set``.
            This method also sets the 'subject_category' and 'object_category'
            edge filters, to get a consistent set of nodes in the subgraph.

        Parameters
        ----------
        key: str
            The key for node filter
        value: Union[str, set]
            The value for the node filter. Can be either a string or a set.

        """
        if key == 'category':
            if isinstance(value, set):
                if 'subject_category' in self.edge_filters:
                    self.edge_filters['subject_category'].update(value)
                else:
                    self.edge_filters['subject_category'] = value
                if 'object_category' in self.edge_filters:
                    self.edge_filters['object_category'].update(value)
                else:
                    self.edge_filters['object_category'] = value
            else:
                raise TypeError("'category' node filter should have a value of type 'set'")

        if key in self.node_filters:
            self.node_filters[key].update(value)
        else:
            self.node_filters[key] = value

    def set_edge_filter(self, key: str, value: set) -> None:
        """
        Set an edge filter, as defined by a key and value pair.
        These filters are used to create a subgraph or reduce the
        search space when fetching edges from a source.

        .. note::
            When defining the 'subject_category' or 'object_category' filter,
            the value should be of type ``set``.
            This method also sets the 'category' node filter, to get a
            consistent set of nodes in the subgraph.

        Parameters
        ----------
        key: str
            The key for edge filter
        value: Union[str, set]
            The value for the edge filter. Can be either a string or a set.

        """
        if key in {'subject_category', 'object_category'}:
            if isinstance(value, set):
                if 'category' in self.node_filters:
                    self.node_filters['category'].update(value)
                else:
                    self.node_filters['category'] = value
            else:
                raise TypeError(f"'{key}' edge filter should have a value of type 'set'")

        if key in self.edge_filters:
            self.edge_filters[key].update(value)
        else:
            self.edge_filters[key] = value

    @staticmethod
    def validate_node(node: dict) -> dict:
        """
        Given a node as a dictionary, check for required properties.
        This method will return the node dictionary with default assumptions applied, if any.

        Parameters
        ----------
        node: dict
            A node represented as a dict

        Returns
        -------
        dict
            A node represented as a dict, with default assumptions applied.

        """
        if len(node) == 0:
            log.debug("Empty node encountered: {}".format(node))
            return node

        if 'id' not in node:
            raise KeyError("node does not have 'id' property: {}".format(node))
        if 'name' not in node:
            log.debug("node does not have 'name' property: {}".format(node))
        if 'category' not in node:
            log.debug("node does not have 'category' property: {}\nUsing {} as default".format(node, Transformer.DEFAULT_NODE_CATEGORY))
            node['category'] = [Transformer.DEFAULT_NODE_CATEGORY]

        return node

    @staticmethod
    def validate_edge(edge: dict) -> dict:
        """
        Given an edge as a dictionary, check for required properties.
        This method will return the edge dictionary with default assumptions applied, if any.

        Parameters
        ----------
        edge: dict
            An edge represented as a dict

        Returns
        -------
        dict
            An edge represented as a dict, with default assumptions applied.
        """

        if 'subject' not in edge:
            raise KeyError("edge does not have 'subject' property: {}".format(edge))
        if 'predicate' not in edge:
            raise KeyError("edge does not have 'predicate' property: {}".format(edge))
        if 'object' not in edge:
            raise KeyError("edge does not have 'object' property: {}".format(edge))

        return edge
