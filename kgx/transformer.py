import networkx as nx
from typing import Union, List, Dict
from .prefix_manager import PrefixManager

SimpleValue = Union[List[str], str]

class Transformer(object):
    """
    Base class for performing a Transformation, this can be

     - from a source to an in-memory property graph (networkx)
     - from an in-memory property graph to a target format or database

    """

    def __init__(self, graph=None):
        """
        Create a new Transformer. This should be called directly on a subclass.

        Optional arg: a Transformer
        """

        if graph is not None:
            self.graph = graph
        else:
            self.graph = nx.MultiDiGraph()

        self.filter = {} # type: Dict[str, SimpleValue]
        self.graph_metadata = {}
        self.prefix_manager = PrefixManager()

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
