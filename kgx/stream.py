from typing import Dict, Generator

from kgx.config import get_logger
from kgx.sink import Sink
from kgx.utils.kgx_utils import validate_node, validate_edge

log = get_logger()


class Stream(object):
    """
    The Stream class is responsible for streaming data from source
    and writing data to a sink.

    The Stream itself is agnostic to the nature of the source
    and sink.
    """

    def __init__(self):
        pass

    def process(self, source: Generator, sink: Sink) -> None:
        """
        This method is responsible for reading from ``source``
        and writing to ``sink`` by calling the relevant methods
        based on the incoming data.

        .. note::
            The streamed data must not be mutated.

        Parameters
        ----------
        source: Generator
            A generator from a Source
        sink: kgx.sink.sink.Sink
            An instance of Sink

        """
        for rec in source:
            print(rec)
            if rec:
                if len(rec) == 4:
                    o = validate_edge(rec[-1])
                    sink.write_edge(o)
                else:
                    o = validate_node(rec[-1])
                    sink.write_node(o)
