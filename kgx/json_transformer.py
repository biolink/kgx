import json
import logging
from typing import List, Dict

from kgx.pandas_transformer import PandasTransformer


class JsonTransformer(PandasTransformer):
    """
    Transformer that parses a JSON, and loads nodes and edges into a networkx.MultiDiGraph
    """

    def parse(self, filename: str, input_format: str = 'json', **kwargs) -> None:
        """
        Parse a JSON file of the format,

        {
            "nodes" : [...],
            "edges" : [...],
        }

        Parameters
        ----------
        filename : str
            JSON file to read from
        input_format : str
            The input file format ('json', by default)
        kwargs: dict
            Any additional arguments

        """
        logging.info("Parsing {}".format(filename))
        with open(filename, 'r') as FH:
            obj = json.load(FH)
            self.load(obj)

    def load(self, obj: Dict[str, List]) -> None:
        """
        Load a JSON object, containing nodes and edges, into a networkx.MultiDiGraph

        Parameters
        ----------
        obj: dict
            JSON Object with all nodes and edges

        """
        if 'nodes' in obj:
            self.load_nodes(obj['nodes'])
        if 'edges' in obj:
            self.load_edges(obj['edges'])

    def load_nodes(self, nodes: List[Dict]) -> None:
        """
        Load a list of nodes into a networkx.MultiDiGraph

        Parameters
        ----------
        nodes: list
            List of nodes

        """
        logging.info("Loading {} nodes into networkx.MultiDiGraph".format(len(nodes)))
        for node in nodes:
            self.load_node(node)

    def load_edges(self, edges: List[Dict]) -> None:
        """
        Load a list of edges into a networkx.MultiDiGraph

        Parameters
        ----------
        edges: list
            List of edges

        """
        logging.info("Loading {} edges into networkx.MultiDiGraph".format(len(edges)))
        for edge in edges:
            self.load_edge(edge)

    def export(self) -> Dict:
        """
        Export networkx.MultiDiGraph as a dictionary.

        Returns
        -------
        dict
            A dictionary with a list nodes and a list of edges

        """
        nodes = []
        edges = []
        for id, data in self.graph.nodes(data=True):
            node = data.copy()
            node['id'] = id
            nodes.append(node)
        for s, o, data in self.graph.edges(data=True):
            edge = data.copy()
            edge['subject'] = s
            edge['object'] = o
            edges.append(edge)

        return {
            'nodes': nodes,
            'edges': edges
        }

    def save(self, filename: str, **kwargs) -> None:
        """
        Write networkx.MultiDiGraph to a file as JSON.

        Parameters
        ----------
        filename: str
            Filename to write to
        kwargs: dict
            Any additional arguments

        """
        obj = self.export()
        with open(filename, 'w') as WH:
            WH.write(json.dumps(obj, indent=4, sort_keys=True))
