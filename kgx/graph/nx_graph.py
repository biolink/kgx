from typing import Dict, Any, Optional, List, Generator

from kgx.graph.base_graph import BaseGraph
from networkx import (
    MultiDiGraph,
    set_node_attributes,
    relabel_nodes,
    set_edge_attributes,
    get_node_attributes,
    get_edge_attributes,
)

from kgx.utils.kgx_utils import prepare_data_dict


class NxGraph(BaseGraph):
    """
    NxGraph is a wrapper that provides methods to interact with a networkx.MultiDiGraph.

    NxGraph extends kgx.graph.base_graph.BaseGraph and implements all the methods from BaseGraph.
    """

    def __init__(self):
        super().__init__()
        self.graph = MultiDiGraph()
        self.name = None

    def add_node(self, node: str, **kwargs: Any) -> None:
        """
        Add a node to the graph.

        Parameters
        ----------
        node: str
            Node identifier
        **kwargs: Any
            Any additional node properties

        """
        if "data" in kwargs:
            data = kwargs["data"]
        else:
            data = kwargs
        self.graph.add_node(node, **data)

    def add_edge(
        self, subject_node: str, object_node: str, edge_key: str = None, **kwargs: Any
    ) -> None:
        """
        Add an edge to the graph.

        Parameters
        ----------
        subject_node: str
            The subject (source) node
        object_node: str
            The object (target) node
        edge_key: Optional[str]
            The edge key
        kwargs: Any
            Any additional edge properties

        """
        if "data" in kwargs:
            data = kwargs["data"]
        else:
            data = kwargs
        return self.graph.add_edge(subject_node, object_node, key=edge_key, **data)

    def add_node_attribute(self, node: str, attr_key: str, attr_value: Any) -> None:
        """
        Add an attribute to a given node.

        Parameters
        ----------
        node: str
            The node identifier
        attr_key: str
            The key for an attribute
        attr_value: Any
            The value corresponding to the key

        """
        self.graph.add_node(node, **{attr_key: attr_value})

    def add_edge_attribute(
        self,
        subject_node: str,
        object_node: str,
        edge_key: Optional[str],
        attr_key: str,
        attr_value: Any,
    ) -> None:
        """
        Add an attribute to a given edge.

        Parameters
        ----------
        subject_node: str
            The subject (source) node
        object_node: str
            The object (target) node
        edge_key: Optional[str]
            The edge key
        attr_key: str
            The attribute key
        attr_value: Any
            The attribute value

        """
        self.graph.add_edge(
            subject_node, object_node, key=edge_key, **{attr_key: attr_value}
        )

    def update_node_attribute(
        self, node: str, attr_key: str, attr_value: Any, preserve: bool = False
    ) -> Dict:
        """
        Update an attribute of a given node.

        Parameters
        ----------
        node: str
            The node identifier
        attr_key: str
            The key for an attribute
        attr_value: Any
            The value corresponding to the key
        preserve: bool
            Whether or not to preserve existing values for the given attr_key

        Returns
        -------
        Dict
            A dictionary corresponding to the updated node properties

        """
        node_data = self.graph.nodes[node]
        updated = prepare_data_dict(
            node_data, {attr_key: attr_value}, preserve=preserve
        )
        self.graph.add_node(node, **updated)
        return updated

    def update_edge_attribute(
        self,
        subject_node: str,
        object_node: str,
        edge_key: Optional[str],
        attr_key: str,
        attr_value: Any,
        preserve: bool = False,
    ) -> Dict:
        """
        Update an attribute of a given edge.

        Parameters
        ----------
        subject_node: str
            The subject (source) node
        object_node: str
            The object (target) node
        edge_key: Optional[str]
            The edge key
        attr_key: str
            The attribute key
        attr_value: Any
            The attribute value
        preserve: bool
            Whether or not to preserve existing values for the given attr_key

        Returns
        -------
        Dict
            A dictionary corresponding to the updated edge properties

        """
        e = self.graph.edges(
            (subject_node, object_node, edge_key), keys=True, data=True
        )
        edge_data = list(e)[0][3]
        updated = prepare_data_dict(edge_data, {attr_key: attr_value}, preserve)
        self.graph.add_edge(subject_node, object_node, key=edge_key, **updated)
        return updated

    def get_node(self, node: str) -> Dict:
        """
        Get a node and its properties.

        Parameters
        ----------
        node: str
            The node identifier

        Returns
        -------
        Dict
            The node dictionary

        """
        n = {}
        if self.graph.has_node(node):
            n = self.graph.nodes[node]
        return n

    def get_edge(
        self, subject_node: str, object_node: str, edge_key: Optional[str] = None
    ) -> Dict:
        """
        Get an edge and its properties.

        Parameters
        ----------
        subject_node: str
            The subject (source) node
        object_node: str
            The object (target) node
        edge_key: Optional[str]
            The edge key

        Returns
        -------
        Dict
            The edge dictionary

        """
        e = {}
        if self.graph.has_edge(subject_node, object_node, edge_key):
            e = self.graph.get_edge_data(subject_node, object_node, edge_key)
        return e

    def nodes(self, data: bool = True) -> Dict:
        """
        Get all nodes in a graph.

        Parameters
        ----------
        data: bool
            Whether or not to fetch node properties

        Returns
        -------
        Dict
            A dictionary of nodes

        """
        return self.graph.nodes(data)

    def edges(self, keys: bool = False, data: bool = True) -> Dict:
        """
        Get all edges in a graph.

        Parameters
        ----------
        keys: bool
            Whether or not to include edge keys
        data: bool
            Whether or not to fetch node properties

        Returns
        -------
        Dict
            A dictionary of edges

        """
        return self.graph.edges(keys=keys, data=data)

    def in_edges(self, node: str, keys: bool = False, data: bool = False) -> List:
        """
        Get all incoming edges for a given node.

        Parameters
        ----------
        node: str
            The node identifier
        keys: bool
            Whether or not to include edge keys
        data: bool
            Whether or not to fetch node properties

        Returns
        -------
        List
            A list of edges

        """
        return self.graph.in_edges(node, keys=keys, data=data)

    def out_edges(self, node: str, keys: bool = False, data: bool = False) -> List:
        """
        Get all outgoing edges for a given node.

        Parameters
        ----------
        node: str
            The node identifier
        keys: bool
            Whether or not to include edge keys
        data: bool
            Whether or not to fetch node properties

        Returns
        -------
        List
            A list of edges

        """
        return self.graph.out_edges(node, keys=keys, data=data)

    def nodes_iter(self) -> Generator:
        """
        Get an iterable to traverse through all the nodes in a graph.

        Returns
        -------
        Generator
            A generator for nodes where each element is a Tuple that
            contains (node_id, node_data)

        """
        for n in self.graph.nodes(data=True):
            yield n

    def edges_iter(self) -> Generator:
        """
        Get an iterable to traverse through all the edges in a graph.

        Returns
        -------
        Generator
            A generator for edges where each element is a 4-tuple that
            contains (subject, object, edge_key, edge_data)

        """
        for u, v, k, data in self.graph.edges(keys=True, data=True):
            yield u, v, k, data

    def remove_node(self, node: str) -> None:
        """
        Remove a given node from the graph.

        Parameters
        ----------
        node: str
            The node identifier

        """
        self.graph.remove_node(node)

    def remove_edge(
        self, subject_node: str, object_node: str, edge_key: Optional[str] = None
    ) -> None:
        """
        Remove a given edge from the graph.

        Parameters
        ----------
        subject_node: str
            The subject (source) node
        object_node: str
            The object (target) node
        edge_key: Optional[str]
            The edge key

        """
        self.graph.remove_edge(subject_node, object_node, edge_key)

    def has_node(self, node: str) -> bool:
        """
        Check whether a given node exists in the graph.

        Parameters
        ----------
        node: str
            The node identifier

        Returns
        -------
        bool
            Whether or not the given node exists

        """
        return self.graph.has_node(node)

    def has_edge(
        self, subject_node: str, object_node: str, edge_key: Optional[str] = None
    ) -> bool:
        """
        Check whether a given edge exists in the graph.

        Parameters
        ----------
        subject_node: str
            The subject (source) node
        object_node: str
            The object (target) node
        edge_key: Optional[str]
            The edge key

        Returns
        -------
        bool
            Whether or not the given edge exists

        """
        return self.graph.has_edge(subject_node, object_node, key=edge_key)

    def number_of_nodes(self) -> int:
        """
        Returns the number of nodes in a graph.

        Returns
        -------
        int

        """
        return self.graph.number_of_nodes()

    def number_of_edges(self) -> int:
        """
        Returns the number of edges in a graph.

        Returns
        -------
        int

        """
        return self.graph.number_of_edges()

    def degree(self):
        """
        Get the degree of all the nodes in a graph.
        """
        return self.graph.degree()

    def clear(self) -> None:
        """
        Remove all the nodes and edges in the graph.
        """
        self.graph.clear()

    @staticmethod
    def set_node_attributes(graph: BaseGraph, attributes: Dict) -> None:
        """
        Set nodes attributes from a dictionary of key-values.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph to modify
        attributes: Dict
            A dictionary of node identifier to key-value pairs

        """
        return set_node_attributes(graph.graph, attributes)

    @staticmethod
    def set_edge_attributes(graph: BaseGraph, attributes: Dict) -> None:
        """
        Set nodes attributes from a dictionary of key-values.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph to modify
        attributes: Dict
            A dictionary of node identifier to key-value pairs

        Returns
        -------
        Any

        """
        return set_edge_attributes(graph.graph, attributes)

    @staticmethod
    def get_node_attributes(graph: BaseGraph, attr_key: str) -> Dict:
        """
        Get all nodes that have a value for the given attribute ``attr_key``.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph to modify
        attr_key: str
            The attribute key

        Returns
        -------
        Dict
            A dictionary where nodes are the keys and the values
            are the attribute values for ``key``

        """
        return get_node_attributes(graph.graph, attr_key)

    @staticmethod
    def get_edge_attributes(graph: BaseGraph, attr_key: str) -> Dict:
        """
        Get all edges that have a value for the given attribute ``attr_key``.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph to modify
        attr_key: str
            The attribute key

        Returns
        -------
        Dict
            A dictionary where edges are the keys and the values
            are the attribute values for ``attr_key``

        """
        return get_edge_attributes(graph.graph, attr_key)

    @staticmethod
    def relabel_nodes(graph: BaseGraph, mapping: Dict) -> None:
        """
        Relabel identifiers for a series of nodes based on mappings.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph to modify
        mapping: Dict
            A dictionary of mapping where the key is the old identifier
            and the value is the new identifier.

        """
        relabel_nodes(graph.graph, mapping, copy=False)
