import networkx as nx
from typing import Union, List, Dict

SimpleValue = Union[List[str], str]

class Transformer(object):
    """
    Base class for performing a Transformation, this can be

     - from a source to an in-memory property graph (networkx)
     - from an in-memory property graph to a target format or database

    """

    def __init__(self, t=None, graph=None):
        """
        Create a new Transformer. This should be called directly on a subclass.

        Optional arg: a Transformer
        """
        if t is None:
            self.graph = nx.MultiDiGraph()
        else:
            self.graph = t.graph
        if graph is not None:
            self.graph = graph
        self.filter = {} # type: Dict[str, SimpleValue]
        self.graph_metadata = {}

    def report(self) -> None:
        g = self.graph
        print('|Nodes|={}'.format(len(g.nodes())))
        print('|Edges|={}'.format(len(g.edges())))


    def set_filter(self, p: str, v: SimpleValue) -> None:
        """
        For pulling from a database endpoint, allow filtering
        for sets of interest

        Parameters:

         - predicate
         - subject_category
         - object_category
         - source
        """
        self.filter[p] = v
