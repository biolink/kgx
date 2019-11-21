import copy

import networkx as nx

from .transformer import Transformer


class NetworkxTransformer(Transformer):
    """
    Base class for networkx transforms
    TODO: use case for this class
    """
    def __init__(self, source_graph: nx.MultiDiGraph = None):
        super().__init__(source_graph)
        raise NotImplementedError('NetworkxTransformer not yet implemented.')


class GraphMLTransformer(NetworkxTransformer):
    """
    I/O for graphml
    TODO: do we need to support GraphML
    """
    def __init__(self, source_graph: nx.MultiDiGraph = None):
        super().__init__(source_graph)
        raise NotImplementedError('GraphMLTransformer not yet implemented.')

    def save(self, path, **kwargs):
        # self.graph contains python lists in its meta obj, but write_graphml function does not know
        # how to serialize python lists, i.e. graphml does not support lists as a valid type for serialization.
        # As a workaround, we converts lists into comma-separated strings.
        # We use deepcopy to avoid modifying original self.graph object.
        dgraph = copy.deepcopy(self.graph)
        for n, nbrs in dgraph.adjacency():
            for nbr, eattr in nbrs.items():
                for entry, adjitem in eattr.items():
                    for prop_uri, obj_curies in adjitem.items():
                        if obj_curies is None:
                            adjitem[prop_uri] = ""
                        else:
                            if isinstance(obj_curies, list):
                                adjitem[prop_uri] = ','.join(obj_curies)
                            else:
                                adjitem[prop_uri] = str(obj_curies)

        nx.write_graphml(dgraph, path)
