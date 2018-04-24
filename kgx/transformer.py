import networkx as nx

class Transformer(object):

    def __init__(self, t=None):
        if t is None:
            self.graph = nx.MultiDiGraph()
        else:
            self.graph = t.graph
        self.filter = {}
        
    def report(self):
        g = self.graph
        print('|Nodes|={}'.format(len(g.nodes())))
        print('|Edges|={}'.format(len(g.edges())))             


    def set_filter(self, p, v):
        """
        For pulling from a database endpoint, allow filtering
        for sets of interest
        
        Parameters:

         - predicate
         - subject_category
         - object_category
        """
        self.filter[p] = v
