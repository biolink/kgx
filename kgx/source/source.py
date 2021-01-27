from typing import Dict, Generator

from kgx import Transformer
from kgx.config import get_logger

log = get_logger()


class Source(object):
    """
    A Source is responsible for reading data as records
    from a store where the store is a file or a database.
    """
    def __init__(self):
        self.graph_metadata: Dict = {}
        self._node_properties = set()
        self._edge_properties = set()

    def parse(self, **kwargs) -> Generator:
        """
        This method reads from the underlying store, using the
        arguments provided in ``config`` and yields records.

        Parameters
        ----------
        **kwargs

        Returns
        -------
        Generator

        """
        pass

    @staticmethod
    def validate_node(node: Dict) -> Dict:
        """
        Given a node as a dictionary, check for required properties.
        This method will return the node dictionary with default
        assumptions applied, if any.

        Parameters
        ----------
        node: Dict
            A node represented as a dict

        Returns
        -------
        Dict
            A node represented as a dict, with default assumptions applied.

        """
        if len(node) == 0:
            log.debug("Empty node encountered: {}".format(node))
        else:
            if 'id' not in node:
                raise KeyError("node does not have 'id' property: {}".format(node))
            if 'name' not in node:
                log.debug("node does not have 'name' property: {}".format(node))
            if 'category' not in node:
                log.debug("node does not have 'category' property: {}\nUsing {} as default".format(node, Transformer.DEFAULT_NODE_CATEGORY))
                node['category'] = [Transformer.DEFAULT_NODE_CATEGORY]
        return node

    @staticmethod
    def validate_edge(edge: Dict) -> Dict:
        """
        Given an edge as a dictionary, check for required properties.
        This method will return the edge dictionary with default
        assumptions applied, if any.

        Parameters
        ----------
        edge: Dict
            An edge represented as a dict

        Returns
        -------
        Dict
            An edge represented as a dict, with default assumptions applied.
        """
        if 'subject' not in edge:
            raise KeyError("edge does not have 'subject' property: {}".format(edge))
        if 'predicate' not in edge:
            raise KeyError("edge does not have 'predicate' property: {}".format(edge))
        if 'object' not in edge:
            raise KeyError("edge does not have 'object' property: {}".format(edge))
        return edge
