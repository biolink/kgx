import logging
from .sink import Sink

class ProgressSink(Sink):
    """
    This wraps another sink in order to print progress tracking statistics.
    """
    def __init__(self, sink, log_threshold=1000):
        self.sink = sink
        self.log_threshold = log_threshold
        self.node_count = 0
        self.edge_count = 0

    def _inc(self, tag):
        attr = tag + '_count'
        count = getattr(self, attr) + 1
        setattr(self, attr, count)
        if count % self.log_threshold == 0:
            logging.info('%ss added: %8d', tag.upper(), count)

    def add_node(self, *args, **kwargs):
        self.sink.add_node(*args, **kwargs)
        self._inc('node')

    def add_edge(self, *args, **kwargs):
        self.sink.add_edge(*args, **kwargs)
        self._inc('edge')
