from typing import Dict, Generator, Any, Union

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
        self._node_properties = set()
        self._edge_properties = set()

    def parse(self, **kwargs: Any) -> Generator:
        """
        This method reads from the underlying store, using the
        arguments provided in ``config`` and yields records.

        Parameters
        ----------
        **kwargs: Any

        Returns
        -------
        Generator

        """
        pass

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

        TODO: test filtering behavior

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
                if k in {'subject_category', 'object_category'}:
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

            # # Check for subject and object filter
            # if self.store.has_node(edge['subject']):
            #     subject_node = self.store.nodes()[edge['subject']]
            # else:
            #     subject_node = None
            #
            # if self.store.has_node(edge['object']):
            #     object_node = self.store.nodes()[edge['object']]
            # else:
            #     object_node = None
            #
            # if 'subject_category' in self.edge_filters:
            #     if self.stream:
            #         log.warning("Cannot filter for subject_category while streaming")
            #     else:
            #         f = self.edge_filters['subject_category']
            #         if subject_node:
            #             # subject node exists in graph
            #             if any(x in subject_node['category'] for x in f):
            #                 pass_filter = True
            #             else:
            #                 return False
            #         else:
            #             # subject node does not exist in graph
            #             return False
            #
            # if 'object_category' in self.edge_filters:
            #     if self.stream:
            #         log.warning("Cannot filter for object_category while streaming")
            #     else:
            #         f = self.edge_filters['object_category']
            #         if object_node:
            #             # object node exists in graph
            #             if any(x in object_node['category'] for x in f):
            #                 pass_filter = True
            #             else:
            #                 return False
            #         else:
            #             # object node does not exist in graph
            #             return False
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

    def set_node_filters(self, filters: Dict) -> None:
        for k, v in filters.items():
            self.set_node_filter(k, v)

    def set_edge_filters(self, filters: Dict) -> None:
        for k, v in filters.items():
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