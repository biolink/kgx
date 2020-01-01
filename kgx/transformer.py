import networkx as nx
import json, time, click, logging
from typing import Union, List, Dict, Tuple
from networkx.readwrite import json_graph

from kgx.utils.graph_utils import get_category_via_superclass
from kgx.utils.kgx_utils import get_toolkit, get_biolink_mapping, sentencecase_to_snakecase

from kgx.mapper import clique_merge

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

    DEFAULT_NODE_LABEL = 'named_thing'

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

    def categorize(self):
        """
        Find and validate category for every node in self.graph
        """
        node_to_categories = {}
        preserve = {}
        for n, data in self.graph.nodes(data=True):
            logging.info("Processing node {}".format(n))
            new_categories = set()
            if 'category' in data:
                categories = data['category']
                preserve[n] = data['category']
                for category in categories:
                    element = get_biolink_mapping(category)
                    if element is not None:
                        # there is a direct mapping to a BioLink Model class
                        mapped_category = element['name']
                        logging.debug("Category: {} has a direct mapping to BioLink Model class {}".format(category, mapped_category))
                        new_categories.update([mapped_category])
                    else:
                        if category in ADDITIONAL_LABELS:
                            element = get_biolink_mapping(ADDITIONAL_LABELS[category])
                            if element is not None:
                                # take a look at an additional list of mappings
                                mapped_category = element['name']
                                logging.debug("Category: {} mapped over to {} has a direct mapping to BioLink Model class {}".format(category, ADDITIONAL_LABELS[category], mapped_category))
                                new_categories.update([mapped_category])
                        else:
                            # subClassOf traversal required
                            # assuming that the graph contains subClassOf edges
                            # and the node subClassOf x
                            new_categories.update(get_category_via_superclass(self.graph, category))
            else:
                # try via subClassOf
                # subClassOf traversal required
                # assuming that the graph contains subClassOf edges
                # and the node subClassOf x
                logging.info("node doesn't have a category field; trying to infer category via subclass_of axiom")
                for u, v, edge_data in self.graph.edges(n, data=True):
                    logging.info("u: {} v: {} data: {}".format(u, v, edge_data))
                    if edge_data['edge_label'] == 'subclass_of':
                        curie = v
                        new_categories.update(get_category_via_superclass(self.graph, curie))

            new_categories = [sentencecase_to_snakecase(x) for x in new_categories]
            if len(new_categories) == 0:
                new_categories.append('named_thing')
            logging.debug("Output categories: {}".format(new_categories))
            node_to_categories[n] = new_categories
        nx.set_node_attributes(self.graph, node_to_categories, 'category')
        nx.set_node_attributes(self.graph, preserve, '_old_category')

    def merge_graphs(self, graphs: List[nx.MultiDiGraph]) -> None:
        """
        Merge all graphs with self.graph

        - If two nodes with same 'id' exist in two graphs, the nodes will be merged based on the 'id'
        - If two nodes with the same 'id' exists in two graphs and they both have conflicting values
        for a property, then the value is overwritten from left to right
        - If two edges with the same 'key' exists in two graphs, the edge will be merged based on the
        'key' property
        - If two edges with the same 'key' exists in two graphs and they both have one or more conflicting
        values for a property, then the value is overwritten from left to right

        Parameters
        ----------
        graphs: List[networkx.MultiDiGraph]
            List of graphs that are to be merged with self.graph

        """
        # TODO: Check behavior and consistency

        graphs.insert(0, self.graph)
        self.graph = nx.compose_all(graphs)

    def remap_node_identifier(self, type: str, new_property: str, prefix=None) -> None:
        """
        Remap a node's 'id' attribute with value from a node's 'new_property' attribute.

        Parameters
        ----------
        type: string
            label referring to nodes whose 'id' needs to be remapped

        new_property: string
            property name from which the new value is pulled from

        prefix: string
            signifies that the value for 'new_property' is a list and the 'prefix' indicates which value
            to pick from the list

        """
        #TODO: test functionality and extend further
        mapping = {}
        for nid, data in self.graph.nodes(data=True):
            node_data = data.copy()
            if type not in node_data['category']:
                continue
            if new_property in node_data:
                if prefix:
                    # data[new_property] contains a list of values
                    new_property_values = node_data[new_property]
                    for v in new_property_values:
                        if prefix in v:
                            # take the first occurring value that contains the given prefix
                            if 'HGNC:HGNC:' in v:
                                # TODO: this is a temporary fix and must be removed later
                                v = ':'.join(v.split(':')[1:])
                            mapping[nid] = v
                            break
                else:
                    # node_data[new_property] contains a string value
                    mapping[nid] = node_data[new_property]
            else:
                # node does not contain new_property key; fall back to original node 'id'
                mapping[nid] = nid

        # TODO: is there a better way to do this in networkx 2.x?
        nx.set_node_attributes(self.graph, values=mapping, name='id')
        nx.relabel_nodes(self.graph, mapping, copy=False)

        # update 'subject' of all outgoing edges
        updated_subject_values = {}
        for edge in self.graph.out_edges(keys=True):
            updated_subject_values[edge] = edge[0]
        nx.set_edge_attributes(self.graph, values=updated_subject_values, name='subject')

        # update 'object' of all incoming edges
        updated_object_values = {}
        for edge in self.graph.in_edges(keys=True):
            updated_object_values[edge] = edge[1]
        nx.set_edge_attributes(self.graph, values=updated_object_values, name='object')

    def remap_node_property(self, type: str, old_property: str, new_property: str) -> None:
        """
        Remap the value in node 'old_property' attribute with value from node 'new_property' attribute.

        Parameters
        ----------
        type: string
            label referring to nodes whose property needs to be remapped

        old_property: string
            old property name whose value needs to be replaced

        new_property: string
            new property name from which the value is pulled from

        """
        # TODO: is there a better way to do this in networkx 2.x?
        mapping = {}
        for nid, data in self.graph.nodes(data=True):
            node_data = data.copy()
            if type not in node_data['category']:
                continue
            if new_property in node_data:
                mapping[nid] = node_data[new_property]
            elif old_property in node_data:
                mapping[nid] = node_data[old_property]
        nx.set_node_attributes(self.graph, values=mapping, name=old_property)

    def remap_edge_property(self, type: str, old_property: str, new_property: str) -> None:
        """
        Remap the value in edge 'old_property' attribute with value from edge 'new_property' attribute.

        Parameters
        ----------
        type: string
            label referring to edges whose property needs to be remapped

        old_property: string
            old property name whose value needs to be replaced

        new_property: string
            new property name from which the value is pulled from

        """
        # TODO: is there a better way to do this in networkx 2.x?
        mapping = {}
        for edge, data in self.graph.edges(data=True, keys=True):
            edge_key = edge[0:3]
            edge_data = data.copy()
            if type not in edge_data['edge_label']:
                continue
            if new_property in edge_data:
                mapping[edge_key] = edge_data[new_property]
            else:
                mapping[edge_key] = edge_data[old_property]
        nx.set_edge_attributes(self.graph, values=mapping, name=old_property)

    @staticmethod
    def dump(g: nx.MultiDiGraph) -> Dict:
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
        json_data = Transformer.dump(g)
        FH.write(json.dumps(json_data))
        FH.close()

    @staticmethod
    def restore(data: Dict) -> nx.MultiDiGraph:
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
        g = Transformer.restore(json.loads(data))
        return g

    @staticmethod
    def current_time_in_millis():
        # TODO: move to Utils (and others)
            return int(round(time.time() * 1000))

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
            logging.warning("Empty node encountered: {}".format(node))
            return node

        if 'id' not in node:
            raise KeyError("node does not have 'id' property: {}".format(node))
        if 'name' not in node:
            logging.warning("node does not have 'name' property: {}".format(node))
        if 'category' not in node:
            logging.warning("node does not have 'category' property: {}\nUsing {} as default".format(node, Transformer.DEFAULT_NODE_LABEL))
            node['category'] = [Transformer.DEFAULT_NODE_LABEL]

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
