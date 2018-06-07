import networkx as nx
import json

from typing import Union, List, Dict
from networkx.readwrite import json_graph

from .prefix_manager import PrefixManager
from .filter import Filter

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


        self.filters = [] # Type: List[Filter]
        self.graph_metadata = {}
        self.prefix_manager = PrefixManager()

    def report(self) -> None:
        g = self.graph
        print('|Nodes|={}'.format(len(g.nodes())))
        print('|Edges|={}'.format(len(g.edges())))

    def is_empty(self) -> bool:
        return len(self.graph.nodes()) == 0 and len(self.graph.edges()) == 0

    def add_filter(self, f:Filter) -> None:
        self.filters.append(f)

    def merge(self, graphs):
        """
        Merge all graphs with self.graph

        - If two nodes with same 'id' exist in two graphs, the nodes will be merged based on the 'id'
        - If two nodes with the same 'id' exists in two graphs and they both have conflicting values
        for a property, then the value is overwritten from left to right
        - If two edges with the same 'key' exists in two graphs, the edge will be merged based on the
        'key' property
        - If two edges with the same 'key' exists in two graphs and they both have one or more conflicting
        values for a property, then the value is overwritten from left to right

        """

        graphs.insert(0, self.graph)
        self.graph = nx.compose_all(graphs, "mergedMultiDiGraph")

    @staticmethod
    def dump(G):
        """
        Convert nx graph G as a JSON dump
        """
        data = json_graph.node_link_data(G)
        return data

    @staticmethod
    def dump_to_file(G, filename):
        """
        Convert nx graph G as a JSON dump and write to file
        """
        FH = open(filename, "w")
        json_data = Transformer.dump(G)
        FH.write(json.dumps(json_data))
        FH.close()
        return json_data

    @staticmethod
    def restore(json_data):
        """
        Create a nx graph with the given JSON data
        """
        G = json_graph.node_link_graph(json_data)
        return G

    @staticmethod
    def restore_from_file(filename):
        """
        Create a nx graph with the given JSON data and write to file
        """
        FH = open(filename, "r")
        data = FH.read()
        G = Transformer.restore(json.loads(data))
        return G
