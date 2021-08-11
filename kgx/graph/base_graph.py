from typing import Dict, Optional, List, Generator, Any


class BaseGraph(object):
    """
    BaseGraph that is a wrapper and provides methods to interact with a graph store.

    All implementations should extend this BaseGraph class and implement all the defined methods.
    """

    def __init__(self):
        self.graph = None
        self.name = None

    def add_node(self, node: str, **kwargs: Any) -> Any:
        """
        Add a node to the graph.

        Parameters
        ----------
        node: str
            Node identifier
        **kwargs: Any
            Any additional node properties

        """
        pass

    def add_edge(
        self,
        subject_node: str,
        object_node: str,
        edge_key: Optional[str] = None,
        **kwargs: Any
    ) -> Any:
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

        Returns
        -------
        Any

        """
        pass

    def add_node_attribute(self, node: str, key: str, value: Any) -> Any:
        """
        Add an attribute to a given node.

        Parameters
        ----------
        node: str
            The node identifier
        key: str
            The key for an attribute
        value: Any
            The value corresponding to the key

        Returns
        -------
        Any

        """
        pass

    def add_edge_attribute(
        self,
        subject_node: str,
        object_node: str,
        edge_key: Optional[str],
        attr_key: str,
        attr_value: Any,
    ) -> Any:
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

        Returns
        -------
        Any

        """
        pass

    def update_node_attribute(self, node, key: str, value: Any) -> Dict:
        """
        Update an attribute of a given node.

        Parameters
        ----------
        node: str
            The node identifier
        key: str
            The key for an attribute
        value: Any
            The value corresponding to the key

        Returns
        -------
        Dict
            A dictionary corresponding to the updated node properties

        """
        pass

    def update_edge_attribute(
        self,
        subject_node: str,
        object_node: str,
        edge_key: Optional[str],
        attr_key: str,
        attr_value: Any,
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

        Returns
        -------
        Dict
            A dictionary corresponding to the updated edge properties

        """
        pass

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
        pass

    def get_edge(
        self, subject_node: str, object_node: str, edge_key: Optional[str]
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
        pass

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
        pass

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
        pass

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
        pass

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
        pass

    def nodes_iter(self) -> Generator:
        """
        Get an iterable to traverse through all the nodes in a graph.

        Returns
        -------
        Generator
            A generator for nodes

        """
        pass

    def edges_iter(self) -> Generator:
        """
        Get an iterable to traverse through all the edges in a graph.

        Returns
        -------
        Generator
            A generator for edges

        """
        for u, v, k, data in self.edges(keys=True, data=True):
            yield (u, v, k, data)

    def remove_node(self, node: str) -> Any:
        """
        Remove a given node from the graph.

        Parameters
        ----------
        node: str
            The node identifier

        Returns
        -------
        Any

        """
        pass

    def remove_edge(
        self, subject_node: str, object_node: str, edge_key: Optional[str] = None
    ) -> Any:
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

        Returns
        -------
        Any

        """
        pass

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
        pass

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
        pass

    def number_of_nodes(self) -> int:
        """
        Returns the number of nodes in a graph.

        Returns
        -------
        int

        """
        pass

    def number_of_edges(self) -> int:
        """
        Returns the number of edges in a graph.

        Returns
        -------
        int

        """
        pass

    def degree(self):
        """
        Get the degree of all the nodes in a graph.
        """
        pass

    def clear(self) -> None:
        """
        Remove all the nodes and edges in the graph.
        """
        pass

    @staticmethod
    def set_node_attributes(graph: Any, attributes: Dict) -> Any:
        """
        Set nodes attributes from a dictionary of key-values.

        Parameters
        ----------
        graph: Any
            The graph to modify
        attributes: Dict
            A dictionary of node identifier to key-value pairs

        Returns
        -------
        Any

        """
        pass

    @staticmethod
    def set_edge_attributes(graph: Any, attributes: Dict) -> Any:
        """
        Set nodes attributes from a dictionary of key-values.

        Parameters
        ----------
        graph: Any
            The graph to modify
        attributes: Dict
            A dictionary of node identifier to key-value pairs

        Returns
        -------
        Any

        """
        pass

    @staticmethod
    def get_node_attributes(graph: Any, attr_key: str) -> Any:
        """
        Get all nodes that have a value for the given attribute ``attr_key``.

        Parameters
        ----------
        graph: Any
            The graph to modify
        attr_key: str
            The attribute key

        Returns
        -------
        Any

        """
        pass

    @staticmethod
    def get_edge_attributes(graph: Any, attr_key: str) -> Any:
        """
        Get all edges that have a value for the given attribute ``attr_key``.

        Parameters
        ----------
        graph: Any
            The graph to modify
        attr_key: str
            The attribute key

        Returns
        -------
        Any

        """
        pass

    @staticmethod
    def relabel_nodes(graph: Any, mapping: Dict) -> Any:
        """
        Relabel identifiers for a series of nodes based on mappings.

        Parameters
        ----------
        graph: Any
            The graph to modify
        mapping: Dict[str, str]
            A dictionary of mapping where the key is the old identifier
            and the value is the new identifier.

        Returns
        -------
        Any

        """
        pass
