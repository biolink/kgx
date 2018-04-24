import pandas as pd
import networkx as nx
from .transformer import Transformer

class NetworkxTransformer(Transformer):
    """
    Base class for networkx transforms
    """
    pass

class GraphMLTransformer(NetworkxTransformer):
    """
    I/O for graphml
    """

    def save(self, path):
        nx.write_graphml(self.graph, path)
