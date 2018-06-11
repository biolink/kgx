import json
import logging
from .transformer import Transformer
from .pandas_transformer import PandasTransformer  # Temp

class JsonTransformer(PandasTransformer):
    """
    """

    def parse(self, filename, **args):
        """
        Parse a JSON file
        """
        with open(filename, 'r') as f:
            obj = json.load(f)
            self.load(obj)

    def load(self, obj):
        if 'nodes' in obj:
            self.load_nodes(obj['nodes'])
        if 'edges' in obj:
            self.load_edges(obj['edges'])

    def load_nodes(self, objs):
        for obj in objs:
            self.load_node(obj)
    def load_edges(self, objs):
        for obj in objs:
            self.load_edge(obj)

    def export(self):
        nodes=[]
        edges=[]
        for id,data in self.graph.nodes_iter(data=True):
            node = data.copy()
            node['id'] = id
            nodes.append(node)
        for o,s,data in self.graph.edges_iter(data=True):
            edge = data.copy()
            edge['subject'] = s
            edge['object'] = o
            edges.append(edge)

        return {
            'nodes':nodes,
            'edges':edges,
            }

    def save(self, filename, **args):
        """
        Write a JSON file
        """
        obj = self.export()
        with open(filename,'w') as file:
            file.write(json.dumps(obj, indent=4, sort_keys=True))
