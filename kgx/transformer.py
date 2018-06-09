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

    def remap_node_identifier(self, type, new_property, prefix=None):
        """
        Remap node `id` attribute with value from node `new_property` attribute

        Parameters
        ----------
        type: string
            label referring to nodes whose id needs to be remapped

        new_property: string
            property name from which the new value is pulled from

        prefix: string
            signifies that the value for `new_property` is a list and the `prefix` indicates which value
            to pick from the list

        """
        mapping = {}
        for node_id in self.graph.nodes_iter():
            node = self.graph.node[node_id]
            if type not in node['labels']:
                continue
            if new_property in node:
                if prefix:
                    # node[new_property] contains a list of values
                    new_property_values = node[new_property]
                    for v in new_property_values:
                        if prefix in v:
                            # take the first occurring value that contains the given prefix
                            if 'HGNC:HGNC:' in v:
                                # TODO: this is a temporary fix and must be removed later
                                v = ':'.join(v.split(':')[1:])
                            mapping[node_id] = v
                            break
                else:
                    # node[new_property] contains a string value
                    mapping[node_id] = node[new_property]
            else:
                # node does not contain new_property key; fall back to original node 'id'
                mapping[node_id] = node_id

        nx.set_node_attributes(self.graph, values = mapping, name = 'id')
        nx.relabel_nodes(self.graph, mapping, copy=False)

        # update 'subject' of all outgoing edges
        updated_subject_values = {}
        for edge in self.graph.out_edges(keys=True):
            updated_subject_values[edge] = edge[0]
        nx.set_edge_attributes(self.graph, values = updated_subject_values, name = 'subject')

        # update 'object' of all incoming edges
        updated_object_values = {}
        for edge in self.graph.in_edges(keys=True):
            updated_object_values[edge] = edge[1]
        nx.set_edge_attributes(self.graph, values = updated_object_values, name = 'object')

    def remap_node_property(self, type, old_property, new_property):
        """
        Remap the value in node `old_property` attribute with value from node `new_property` attribute

        Parameters
        ----------
        type: string
            label referring to nodes whose property needs to be remapped

        old_property: string
            old property name whose value needs to be replaced

        new_property: string
            new property name from which the value is pulled from

        """
        mapping = {}
        for node_id in self.graph.nodes_iter():
            node = self.graph.node[node_id]
            if type not in node['labels']:
                continue
            if new_property in node:
                mapping[node_id] = node[new_property]
            elif old_property in node:
                mapping[node_id] = node[old_property]
        nx.set_node_attributes(self.graph, values = mapping, name = old_property)

    def remap_edge_property(self, type, old_property, new_property):
        """
        Remap the value in edge `old_property` attribute with value from edge `new_property` attribute

        Parameters
        ----------
        type: string
            label referring to edges whose property needs to be remapped

        old_property: string
            old property name whose value needs to be replaced

        new_property: string
            new property name from which the value is pulled from

        """
        mapping = {}
        for edge in self.graph.edges_iter(data=True, keys=True):
            edge_key = edge[0:3]
            edge_data = edge[3]
            if type not in edge_data['edge_label']:
                continue
            if new_property in edge_data:
                mapping[edge_key] = edge_data[new_property]
            else:
                mapping[edge_key] = edge_data[old_property]
        nx.set_edge_attributes(self.graph, values = mapping, name = old_property)

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
