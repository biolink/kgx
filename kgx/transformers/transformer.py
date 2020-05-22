import networkx as nx
import json
import time
import logging
from typing import Union, List, Dict, Tuple
from networkx.readwrite import json_graph


SimpleValue = Union[List[str], str]

IGNORE_CLASSES = ['All', 'entity']

ADDITIONAL_LABELS = {
    'phenotypic_abnormality': 'phenotypic_feature',
    'clinical_course': 'phenotypic_feature',
    'blood_group': 'phenotypic_feature',
    'clinical_modifier': 'phenotypic_feature',
    'frequency': 'phenotypic_feature',
    'mode_of_inheritance': 'phenotypic_feature',
    'past_medical_history': 'phenotypic_feature'
}


class Transformer(object):
    """
    Base class for performing a transformation.

    This can be,
     - from a source to an in-memory property graph (networkx.MultiDiGraph)
     - from an in-memory property graph to a target format or database (Neo4j, CSV, RDF Triple Store, TTL)
    """

    DEFAULT_NODE_CATEGORY = 'biolink:NamedThing'

    def __init__(self, source_graph: nx.MultiDiGraph = None):
        if source_graph:
            self.graph = source_graph
        else:
            self.graph = nx.MultiDiGraph()

        self.filters = {}
        self.graph_metadata = {}

    def report(self) -> None:
        """
        Print a summary report about self.graph
        """
        logging.info('Total nodes in {}: {}'.format(self.graph.name or 'graph', len(self.graph.nodes())))
        logging.info('Total edges in {}: {}'.format(self.graph.name or 'graph', len(self.graph.edges())))

    def is_empty(self) -> bool:
        """
        Check whether self.graph is empty.

        Returns
        -------
        bool
            A boolean value asserting whether the graph is empty or not

        """
        return len(self.graph.nodes()) == 0 and len(self.graph.edges()) == 0

    def set_filter(self, key: str, value: SimpleValue) -> None:
        """
        Set a filter, defined by a key and value pair.
        These filters are used to reduce the search space.

        Parameters
        ----------
        key: str
            The key for a filter
        value: Union[List[str], str]
            The value for a filter. Can be either a string or a list

        """
        self.filters[key] = value

    @staticmethod
    def serialize(g: nx.MultiDiGraph) -> Dict:
        """
        Convert networkx.MultiDiGraph as a dictionary.

        Parameters
        ----------
        g: networkx.MultiDiGraph
            Graph to convert as a dictionary

        Returns
        -------
        dict
            A dictionary

        """
        data = json_graph.node_link_data(g)
        return data

    @staticmethod
    def dump_to_file(g: nx.MultiDiGraph, filename: str) -> None:
        """
        Serialize networkx.MultiDiGraph as JSON and write to file.

        Parameters
        ----------
        g: networkx.MultiDiGraph
            Graph to convert as a dictionary
        filename: str
            File to write the JSON

        """
        FH = open(filename, "w")
        json_data = Transformer.serialize(g)
        FH.write(json.dumps(json_data))
        FH.close()

    @staticmethod
    def deserialize(data: Dict) -> nx.MultiDiGraph:
        """
        Deserialize a networkx.MultiDiGraph from a dictionary.

        Parameters
        ----------
        data: dict
            Dictionary containing nodes and edges

        Returns
        -------
        networkx.MultiDiGraph
            A networkx.MultiDiGraph representation

        """
        g = json_graph.node_link_graph(data)
        return g

    @staticmethod
    def restore_from_file(filename) -> nx.MultiDiGraph:
        """
        Deserialize a networkx.MultiDiGraph from a JSON file.

        Parameters
        ----------
        filename: str
            File to read from

        Returns
        -------
        networkx.MultiDiGraph
            A networkx.MultiDiGraph representation

        """
        FH = open(filename, "r")
        data = FH.read()
        g = Transformer.deserialize(json.loads(data))
        return g

    @staticmethod
    def validate_node(node: dict) -> dict:
        """
        Given a node as a dictionary, check for required properties.
        This method will return the node dictionary with default assumptions applied, if any.

        Parameters
        ----------
        node: dict
            A node represented as a dict

        Returns
        -------
        dict
            A node represented as a dict, with default assumptions applied.

        """
        if len(node) == 0:
            logging.debug("Empty node encountered: {}".format(node))
            return node

        if 'id' not in node:
            raise KeyError("node does not have 'id' property: {}".format(node))
        if 'name' not in node:
            logging.debug("node does not have 'name' property: {}".format(node))
        if 'category' not in node:
            logging.warning("node does not have 'category' property: {}\nUsing {} as default".format(node, Transformer.DEFAULT_NODE_CATEGORY))
            node['category'] = [Transformer.DEFAULT_NODE_CATEGORY]

        return node

    @staticmethod
    def validate_edge(edge: dict) -> dict:
        """
        Given an edge as a dictionary, check for required properties.
        This method will return the edge dictionary with default assumptions applied, if any.

        Parameters
        ----------
        edge: dict
            An edge represented as a dict

        Returns
        -------
        dict
            An edge represented as a dict, with default assumptions applied.
        """

        if 'subject' not in edge:
            raise KeyError("edge does not have 'subject' property: {}".format(edge))
        if 'edge_label' not in edge:
            raise KeyError("edge does not have 'edge_label' property: {}".format(edge))
        if 'object' not in edge:
            raise KeyError("edge does not have 'object' property: {}".format(edge))

        return edge
