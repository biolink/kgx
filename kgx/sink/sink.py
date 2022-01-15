from typing import Dict

from kgx.prefix_manager import PrefixManager


class Sink(object):
    """
    A Sink is responsible for writing data as records
    to a store where the store is a file or a database.

    Parameters:
    ----------
    :param owner: Transformer
        Transformer to which the GraphSink belongs
    """

    def __init__(self, owner):
        self.owner = owner
        self.prefix_manager = PrefixManager()
        self.node_properties = set()
        self.edge_properties = set()

    def set_reverse_prefix_map(self, m: Dict) -> None:
        """
        Update default reverse prefix map.

        Parameters
        ----------
        m: Dict
            A dictionary with IRI to prefix mappings

        """
        self.prefix_manager.update_reverse_prefix_map(m)

    def write_node(self, record) -> None:
        """
        Write a node record to the underlying store.

        Parameters
        ----------
        record: Any
            A node record

        """
        pass

    def write_edge(self, record) -> None:
        """
        Write an edge record to the underlying store.

        Parameters
        ----------
        record: Any
            An edge record

        """
        pass

    def finalize(self) -> None:
        """
        Operations that ought to be done after
        writing all the incoming data should be called
        by this method.

        """
        pass
