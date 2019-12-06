import logging
from .sink import Sink

class DebugSink(Sink):
    def __init__(self, limit=None):
        self.limit = limit

    def _limit(self):
        if self.limit is not None:
            self.limit -= 1
            if self.limit <= 0:
                raise RuntimeError('limit exceeded')

    def add_node(self, node_id, attributes):
        logging.debug('NODE: %s %s', node_id, attributes)
        self._limit()

    def add_edge(self, subject_id, object_id, attributes):
        logging.debug('EDGE: %s %s %s', subject_id, object_id, attributes)
        self._limit()
