import networkx as nx
import json, time, bmt

from typing import Union, List, Dict, Tuple
from networkx.readwrite import json_graph

from .prefix_manager import PrefixManager
from .filter import Filter

import click

from kgx.utils.ontology import find_superclass, subclasses
from kgx.utils.str_utils import fmt_edgelabel, fmt_category

from kgx.mapper import clique_merge

SimpleValue = Union[List[str], str]

IGNORE_CLASSSES = ['All', 'entity']

def edges(graph, node=None, mode='all', **attributes) -> List[Tuple[str, str]]:
    """
    A convenient method for getting a list of edges that match the given fitlers
    """
    if mode == 'all':
        method = graph.edges
    elif mode == 'in':
        method = graph.in_edges
    elif mode == 'out':
        method = graph.out_edges:
    else:
        raise Exception('Invalid mode: expected "all", "in", "out", got "{}"'.format(mode))
    edges = []
    for u, v, d in method(nbunch=node, data=True):
        if all(attributes.get(k) == d.get(k) for k in attributes.keys()):
            edges.add((u, v))
    return edges

class Transformer(object):
    """
    Base class for performing a Transformation, this can be

     - from a source to an in-memory property graph (networkx)
     - from an in-memory property graph to a target format or database

    """

    def __init__(self, source=None):
        """
        Create a new Transformer. This should be called directly on a subclass.

        Optional arg: a Transformer
        """

        if isinstance(source, Transformer):
            self.graph = source.graph
        elif isinstance(source, nx.MultiDiGraph):
            self.graph = source
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

    def set_filter(self, target: str, value: SimpleValue) -> None:
        self.filters.append(Filter(target, value))

    def categorize(self, ignore:List[str]=None):
        """
        Attempts to find node categories and edge labels by following
        subclass_of paths within the graph. If a superclass is being ignored
        then all of its children subclasses will be used unless they are also
        being ignored. You can use the ignore feature in this way to get more
        refined categories.

        First builds the `ontology_graph`, which contains all the subclass_of
        relations in the given graph, including implicit ones that can be
        extracted from the `type` node property, and from equivalent nodes.
        Then crawls down from superclasses in the `ontology_graph` to find all
        related nodes that should be categorized under them in the given graph.

        Parameters
        ----------
        ignore: List[str]
            the names of categories to ignore, by default "All" and "entity" are
            ignored.
        """
        if ignore is None:
            ignore = IGNORE_CLASSSES

        ontology_graph = nx.DiGraph()

        with click.progressbar(edges(self.graph, edge_label='subclass_of'), label='Building ontology graph 1/3') as bar:
            for u, v in bar:
                ontology_graph.add_edge(u, v)

        with click.progressbar(edges(self.graph, edge_label='same_as'), label='Building ontology graph 2/3') as bar:
            for u, v, in bar:
                for _, superclass in edges(self.graph, node=u, mode='out', edge_label='subclass_of'):
                    ontology_graph.add_edge(u, superclass)
                for _, superclass in edges(self.graph, node=v, mode='out', edge_label='subclass_of'):
                    ontology_graph.add_edge(v, superclass)

        with click.progressbar(self.graph.nodes(data='type'), label='Building ontology graph 3/3'):
            for n, node_type in bar:
                if node_type in self.graph:
                    ontology_graph.add_edge(n, node_type)

        superclasses = set()

        with click.progressbar(self.graph.nodes(data='name'), label='Finding superclasses') as bar:
            for n, name in bar:
                if name is None:
                    continue

                in_degree = sum(1 for _, _, edge_label in self.graph.in_edges(n, data='edge_label') if edge_label == 'subclass_of')
                out_degree = sum(1 for _, _, edge_label in self.graph.out_edges(n, data='edge_label') if edge_label == 'subclass_of')
                if out_degree == 0 and in_degree > 0:
                    superclasses.add(n)

                c = bmt.get_class(name)
                if c is not None:
                    superclasses.add(n)

                c = bmt.get_by_mapping(n)
                if c is not None:
                    superclasses.add(n)

        def get_valid_superclasses(superclasses):
            """
            Returns a list of the most immediate valid subclasses of the given
            superclasses (which will be a given superclass if it is valid).
            """
            result = set()
            for superclass in superclasses:
                name = self.graph.node[superclass].get('name')
                is_invalid = name is None or name in ignore
                if is_invalid:
                    for subclass, _, edge_label in self.graph.in_edges(superclass, data='edge_label'):
                        if edge_label == 'subclass_of':
                            result.update(get_valid_superclasses([subclass]))
                else:
                    result.add(superclass)
            return result

        superclasses = get_valid_superclasses(superclasses)

        with click.progressbar(superclasses, label='Categorizing nodes') as bar:
            for superclass in bar:
                name = fmt_category(self.graph.node[superclass].get('name'))
                if name is None or name in ignore:
                    continue
                for subclass in subclasses(superclass, ontology_graph):
                    if subclass in self.graph:
                        category = self.graph.node[subclass].get('category')
                        if not isinstance(category, list) or category == [] or category == ['named thing']:
                            self.graph.node[subclass]['category'] = [name]
                        else:
                            if name not in category:
                                category.append(name)

        memo = {}
        # Starts with each uncategorized ge and finds a superclass
        with click.progressbar(self.graph.edges(data=True), label='Categorizing edges') as bar:
            for u, v, data in bar:
                if data.get('edge_label') is None or data['edge_label'] == 'related_to':
                    relation = data.get('relation')

                    if relation not in memo:
                        superclass = find_superclass(relation, self.graph)
                        memo[relation] = fmt_edgelabel(superclass)

                    if memo[relation] is not None:
                        data['edge_label'] = memo[relation]

        # Set all null categories to the default value, and format all others
        with click.progressbar(self.graph.nodes(data='category'), label='Ensuring valid categories') as bar:
            for n, category in bar:
                if not isinstance(category, list) or category == []:
                    self.graph.node[n]['category'] = ['named thing']
                else:
                    category = [fmt_category(c) for c in category]

        # Set all null edgelabels to the default value, and format all others
        with click.progressbar(self.graph.edges(data=True), label='Ensuring valid edge labels') as bar:
            for u, v, data in bar:
                if 'edge_label' not in data or data['edge_label'] is None:
                    data['edge_label'] = 'related_to'
                else:
                    data['edge_label'] = fmt_edgelabel(data['edge_label'])

    def merge_cliques(self, categorize_first=True):
        """
        Merges all nodes that are connected by `same_as` edges, or are marked
        as equivalent by a nodes `same_as` property.

        The clique leader chosen depends on the categories within that clique.
        For this reason it's usually useful to run the `categorize` method first.
        """
        if categorize_first:
            self.categorize()
        self.graph = clique_merge(self.graph)

    def merge_graphs(self, graphs):
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
            if type not in node['category']:
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
            if type not in node['category']:
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

    @staticmethod
    def current_time_in_millis():
            return int(round(time.time() * 1000))
