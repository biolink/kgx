import networkx as nx
import logging, click, bmt, pandas

from collections import defaultdict
from typing import Union, List

bmt.load('https://biolink.github.io/biolink-model/biolink-model.yaml')

def map_graph(G, mapping, preserve=True):
    if preserve:
        for nid in G.nodes_iter():
            if nid in mapping:
                # add_node will append attributes
                G.add_node(nid, source_curie=nid)
        for oid,sid in G.edges_iter():
            if oid in mapping:
                for ex in G[oid][sid]:
                    G[oid][sid][ex].update(source_object=oid)
            if sid in mapping:
                for ex in G[oid][sid]:
                    G[oid][sid][ex].update(source_subject=oid)
    nx.relabel_nodes(G, mapping, copy=False)

def relabel_nodes(graph:nx.Graph, mapping:dict) -> nx.Graph:
    """
    Performs the relabelling of nodes, and ensures that list attributes are
    copied over.

    Example:
        graph = nx.Graph()

        graph.add_edge('a', 'b')
        graph.add_edge('c', 'd')

        graph.node['a']['name'] = ['A']
        graph.node['b']['name'] = ['B']
        graph.node['c']['name'] = ['C']
        graph.node['d']['name'] = ['D']

        graph = relabel_nodes(graph, {'c' : 'b'})

        for n in graph.nodes():
            print(n, graph.node[n])
    Output:
        a {'name': ['A']}
        b {'name': ['B', 'C']}
        d {'name': ['D']}
    """
    print('relabelling nodes...')
    g = nx.relabel_nodes(graph, mapping, copy=True)

    with click.progressbar(graph.nodes(), label='concatenating list attributes') as bar:
        for n in bar:
            if n not in mapping or n == mapping[n]:
                continue

            new_attr_dict = g.node[mapping[n]]
            old_attr_dict = graph.node[n]

            for key, value in old_attr_dict.items():
                if key in new_attr_dict:
                    is_list = \
                        isinstance(new_attr_dict[key], (list, set, tuple)) \
                        and isinstance(old_attr_dict[key], (list, set, tuple))
                    if is_list:
                        s = set(new_attr_dict[key])
                        s.update(old_attr_dict[key])
                        new_attr_dict[key] = list(s)
                else:
                    new_attr_dict[key] = value
    return g

def listify(o:object) -> Union[list, set, tuple]:
    if isinstance(o, (list, set, tuple)):
        return o
    else:
        return [o]

def get_prefix(curie:str, default=None) -> str:
    if ':' in curie:
        prefix, _ = curie.rsplit(':', 1)
        return prefix
    else:
        return default

def build_sort_key(list_of_prefixes:List[List[str]]):
    """
    For a list of lists of prefixes, gets the lowest
    index of a matching prefix.
    """
    def key(n):
        k = len(list_of_prefixes) + 1
        p = get_prefix(n, default='').upper()
        for prefixes in list_of_prefixes:
            for i, prefix in enumerate(prefixes):
                if p == prefix.upper():
                    if i < k:
                        k = i
        return k
    return key

class ReportBuilder(object):
    def __init__(self, graph):
        self.graph = graph
        self.records = []
    def add(self, node, xref):
        provided_by = self.graph.node[node].get('provided_by')
        if provided_by is not None:
            provided_by = '; '.join(provided_by)
        self.records.append({
            'node' : node,
            'xref' : xref,
            'provided_by' : provided_by,
        })

    def to_csv(self, path, **kwargs):
        df = pandas.DataFrame(self.records)
        df = df[['node', 'xref', 'provided_by']]

        if 'index' not in kwargs:
            kwargs['index'] = False

        df.to_csv(path, **kwargs)

def update(d:dict, key, value):
    if key is None or value is None:
        return

    if isinstance(value, list):
        for v in value:
            update(d, key, v)

    if key in d:
        if isinstance(d, list):
            if value not in d[key]:
                d[key].append(value)
        elif d[key] != value:
            d[key] = [d[key], value]
    else:
        d[key] = value

def build_clique_graph(graph:nx.Graph) -> nx.Graph:
    """
    Builds a graph induced by `same_as` relationships.
    """

    cliqueGraph = nx.Graph()

    with click.progressbar(graph.nodes(), label='building cliques') as bar:
        for n in bar:
            attr_dict = graph.node[n]
            if 'same_as' in attr_dict:
                for m in attr_dict['same_as']:
                    cliqueGraph.add_edge(n, m, provided_by=attr_dict['provided_by'])
                    for key, value in graph.node[n].items():
                        update(cliqueGraph.node[n], key, value)
                    update(cliqueGraph.node[n], 'is_node', True)

    return cliqueGraph

def clique_merge(graph:nx.Graph, report=True) -> nx.Graph:
    """
    Builds up cliques using the `same_as` attribute of each node. Uses those
    cliques to build up a mapping for relabelling nodes. Chooses labels so as
    to preserve the original nodes, rather than taking xrefs that don't appear
    as nodes in the graph.

    This method will also expand the `same_as` attribute of the nodes to
    include the discovered clique.
    """

    builder = ReportBuilder(graph)

    original_size = len(graph)
    print('original graph has {} nodes'.format(original_size))

    cliqueGraph = nx.Graph()

    with click.progressbar(graph.nodes(), label='building cliques') as bar:
        for n in bar:
            attr_dict = graph.node[n]
            if 'same_as' in attr_dict:
                for m in attr_dict['same_as']:
                    cliqueGraph.add_edge(n, m)
                    if report:
                        builder.add(n, m)

    if report:
        builder.to_csv('clique-merge-report.csv')

    mapping = {}

    connected_components = list(nx.connected_components(cliqueGraph))

    print('Discovered {} cliques'.format(len(connected_components)))

    with click.progressbar(connected_components, label='building mapping') as bar:
        for nodes in bar:
            nodes = list(nodes)
            categories = set()
            for n in nodes:
                if not graph.has_node(n):
                    continue

                attr_dict = graph.node[n]

                attr_dict['same_as'] = nodes

                if 'category' in attr_dict:
                    categories.update(listify(attr_dict['category']))

                if 'categories' in attr_dict:
                    categories.update(listify(attr_dict['categories']))

            list_of_prefixes = []
            for category in categories:
                try:
                    list_of_prefixes.append(bmt.get_element(category).id_prefixes)
                except:
                    pass

            nodes.sort()
            nodes.sort(key=build_sort_key(list_of_prefixes))

            for n in nodes:
                if n != nodes[0]:
                    mapping[n] = nodes[0]

    g = relabel_nodes(graph, mapping)

    final_size = len(g)
    print('Resulting graph has {} nodes'.format(final_size))
    print('Eliminated {} nodes'.format(original_size - final_size))

    return g
