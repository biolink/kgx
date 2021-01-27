from kgx.graph.nx_graph import NxGraph

from kgx.sink.sink import Sink
from kgx.utils.kgx_utils import generate_edge_key


class NxGraphSink(Sink):

    def __init__(self):
        super().__init__()
        self.graph = NxGraph()

    def write(self, record):
        pass

    def write_node(self, record):
        self.graph.add_node(record['id'], **record)

    def write_edge(self, record):
        key = record['key'] if 'key' in record else generate_edge_key(record['subject'], record['predicate'], record['object'])
        self.graph.add_edge(record['subject'], record['object'], key, **record)
