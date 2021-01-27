from typing import Dict, Generator

from kgx.sink import Sink
from kgx.source import Source


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
                    o = self.process_edge(rec[0], rec[1], rec[2], rec[3])
                    if o:
                        sink.write_edge(o)
                else:
                    o = self.process_node(rec[0], rec[1])
                    if o:
                        sink.write_node(o)
        # TODO: the job of calling finalize has been pushed up to Transformer
        #  This change will affect NtSink, NeoSink, and a few others

    def process_node(self, n: str, data: Dict) -> Dict:
        """
        Process a node record.

        Parameters
        ----------
        n: str
            Node identifier
        data: Dict
            Node data

        """
        node = {}
        if self.check_node_filter:
            node = data
        return node

    def process_edge(self, s: str, o: str, key: str, data: Dict) -> Dict:
        """
        Process an edge record.

        Parameters
        ----------
        s: str
            Subject identifier
        o: str
            Object identifier
        key: str
            Edge key
        data: Dict
            Edge data

        """
        return data


    ## EXPR

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