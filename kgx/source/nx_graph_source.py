from itertools import chain

from kgx.graph.nx_graph import NxGraph

from kgx.source.source import Source


class NxGraphSource(Source):

    def __init__(self):
        super().__init__()
        self.graph = NxGraph()

    def parse(self, graph):
        self.graph = graph
        nodes = self.parse_nodes()
        edges = self.parse_edges()
        return chain(nodes, edges)

    def parse_nodes(self):
        for n, data in self.graph.nodes(data=True):
            yield n, data

    def parse_edges(self):
        for u, v, k, data in self.graph.edges(keys=True, data=True):
            yield u, v, k, data
