import networkx as nx

class Transformer(object):

    def __init__(self, t=None):
        if t is None:
            self.graph = nx.MultiDiGraph()
        else:
            self.graph = t.graph

    def report(self):
        g = self.graph
        print('|Nodes|='.format(len(g.nodes())))
        print('|Edges|='.format(len(g.edges())))             
