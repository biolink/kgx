from typing import Dict, Union, Optional

from kgx.utils.infores import InfoResContext
from kgx.prefix_manager import PrefixManager
from kgx.config import get_logger

log = get_logger()


class Source(object):
    """
    A Source is responsible for reading data as records
    from a store where the store is a file or a database.
    """

    def __init__(self):
        self.graph_metadata: Dict = {}
        self.node_filters = {}
        self.edge_filters = {}
        self.node_properties = set()
        self.edge_properties = set()
        self.prefix_manager = PrefixManager()
        self.infores_context: Optional[InfoResContext] = None

    def set_prefix_map(self, m: Dict) -> None:
        """
        Update default prefix map.

        Parameters
        ----------
        m: Dict
            A dictionary with prefix to IRI mappings

        """
        self.prefix_manager.update_prefix_map(m)

    def check_node_filter(self, node: Dict) -> bool:
        """
        Check if a node passes defined node filters.

        Parameters
        ----------
        node: Dict
            A node

        Returns
        -------
        bool
            Whether the given node has passed all defined node filters

        """
        pass_filter = False
        if self.node_filters:
            for k, v in self.node_filters.items():
                if k in node:
                    # filter key exists in node
                    if isinstance(v, (list, set, tuple)):
                        if any(x in node[k] for x in v):
                            pass_filter = True
                        else:
                            return False
                    elif isinstance(v, str):
                        if node[k] == v:
                            pass_filter = True
                        else:
                            return False
                    else:
                        log.error(f"Unexpected {k} node filter of type {type(v)}")
                        return False
                else:
                    # filter key does not exist in node
                    return False
        else:
            # no node filters defined
            pass_filter = True
        return pass_filter

    def check_edge_filter(self, edge: Dict) -> bool:
        """
        Check if an edge passes defined edge filters.

        Parameters
        ----------
        edge: Dict
            An edge

        Returns
        -------
        bool
            Whether the given edge has passed all defined edge filters

        """
        pass_filter = False
        if self.edge_filters:
            for k, v in self.edge_filters.items():
                if k in {"subject_category", "object_category"}:
                    pass_filter = True
                    continue
                if k in edge:
                    # filter key exists in edge
                    if isinstance(v, (list, set, tuple)):
                        if any(x in edge[k] for x in v):
                            pass_filter = True
                        else:
                            return False
                    elif isinstance(v, str):
                        if edge[k] == v:
                            pass_filter = True
                        else:
                            return False
                    else:
                        log.error(f"Unexpected {k} edge filter of type {type(v)}")
                        return False
                else:
                    # filter does not exist in edge
                    return False
        else:
            # no edge filters defined
            pass_filter = True
        return pass_filter

    def set_node_filter(self, key: str, value: Union[str, set]) -> None:
        """
        Set a node filter, as defined by a key and value pair.
        These filters are used to filter (or reduce) the
        search space when fetching nodes from the underlying store.

        .. note::
            When defining the 'category' filter, the value should be of type ``set``.
            This method also sets the 'subject_category' and 'object_category'
            edge filters, to get a consistent set of nodes in the subgraph.

        Parameters
        ----------
        key: str
            The key for node filter
        value: Union[str, set]
            The value for the node filter.
            Can be either a string or a set.

        """
        if key == "category":
            if isinstance(value, set):
                if "subject_category" in self.edge_filters:
                    self.edge_filters["subject_category"].update(value)
                else:
                    self.edge_filters["subject_category"] = value
                if "object_category" in self.edge_filters:
                    self.edge_filters["object_category"].update(value)
                else:
                    self.edge_filters["object_category"] = value
            else:
                raise TypeError(
                    "'category' node filter should have a value of type 'set'"
                )

        if key in self.node_filters:
            self.node_filters[key].update(value)
        else:
            self.node_filters[key] = value

    def set_node_filters(self, filters: Dict) -> None:
        """
        Set node filters.

        Parameters
        ----------
        filters: Dict
            Node filters

        """
        if filters:
            for k, v in filters.items():
                if isinstance(v, (list, set, tuple)):
                    self.set_node_filter(k, set(v))
                else:
                    self.set_node_filter(k, v)

    def set_edge_filters(self, filters: Dict) -> None:
        """
        Set edge filters.

        Parameters
        ----------
        filters: Dict
            Edge filters

        """
        if filters:
            for k, v in filters.items():
                if isinstance(v, (list, set, tuple)):
                    self.set_edge_filter(k, set(v))
                else:
                    self.set_edge_filter(k, v)

    def set_edge_filter(self, key: str, value: set) -> None:
        """
        Set an edge filter, as defined by a key and value pair.
        These filters are used to filter (or reduce) the
        search space when fetching nodes from the underlying store.

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
            The value for the edge filter.
            Can be either a string or a set.

        """
        if key in {"subject_category", "object_category"}:
            if isinstance(value, set):
                if "category" in self.node_filters:
                    self.node_filters["category"].update(value)
                else:
                    self.node_filters["category"] = value
            else:
                raise TypeError(
                    f"'{key}' edge filter should have a value of type 'set'"
                )

        if key in self.edge_filters:
            self.edge_filters[key].update(value)
        else:
            self.edge_filters[key] = value

    def clear_graph_metadata(self):
        """
        Clears a Source graph's internal graph_metadata. The value of such graph metadata is (now)
        generally a Callable function. This operation can be used in the code when the metadata is
        no longer needed, but may cause peculiar Python object persistent problems downstream.
        """
        self.infores_context = None

    def set_provenance_map(self, kwargs):
        """
        Set up a provenance (Knowledge Source to InfoRes) map
        """
        self.infores_context = InfoResContext()
        self.infores_context.set_provenance_map(kwargs)

    def get_infores_catalog(self) -> Dict[str, str]:
        """
        Return the InfoRes Context of the source
        """
        if not self.infores_context:
            return dict()
        return self.infores_context.get_catalog()

    def set_node_provenance(self, node_data):
        """
        Set a specific node provenance value.
        """
        self.infores_context.set_node_provenance(node_data)

    def set_edge_provenance(self, edge_data):
        """
        Set a specific edge provenance value.
        """
        self.infores_context.set_edge_provenance(edge_data)
