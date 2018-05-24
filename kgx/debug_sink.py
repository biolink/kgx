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
        #print('NODE:', node_id, attributes)
        self._limit()

    def add_edge(self, subject_id, object_id, attributes):
        #print('EDGE:', subject_id, object_id, attributes)
        self._limit()
