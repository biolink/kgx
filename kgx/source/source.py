from typing import Dict, Generator, Any

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
