from typing import Any, Optional, Dict

import jsonstreams

from kgx.config import get_logger
from kgx.sink import Sink


log = get_logger()


class JsonSink(Sink):
    """
    JsonSink is responsible for writing data as records
    to a JSON.

    """

    def __init__(self, filename: str, format: str = 'json', compression: Optional[str] = None, **kwargs: Any):
        super().__init__()
        if compression:
            log.warning("Compression not supported")

        self.FH = jsonstreams.Stream(jsonstreams.Type.object, filename=filename)
        self.NH = None
        self.EH = None

    def write_node(self, record: Dict) -> None:
        """
        Write a node record to JSON.

        Parameters
        ----------
        record: Any
            A node record

        """
        if not self.NH:
            self.NH = self.FH.subarray('nodes')
        self.NH.write(record)

    def write_edge(self, record: Dict) -> None:
        """
        Write an edge record to JSON.

        Parameters
        ----------
        record: Any
            An edge record

        """
        if not self.EH:
            self.EH = self.FH.subarray('edges')
        self.EH.write(record)

    def finalize(self) -> None:
        """
        Finalize by closing the filehandle.
        """
        pass

