import networkx as nx
import logging, click, pandas

from prefixcommons.curie_util import expand_uri
from collections import defaultdict
from typing import Union, List
from bmt import Toolkit

toolkit = None

def get_toolkit():
    """
    This method allows us to load the biolink model toolkit when it's needed,
    and not whenever this file is imported.
    """
    global toolkit

    if toolkit is None:
        toolkit = Toolkit()

    return toolkit

def map_graph(G, mapping, preserve=True):
    if preserve:
        for nid in G.nodes():
            if nid in mapping:
                # add_node will append attributes
                G.add_node(nid, source_curie=nid)
        for oid,sid in G.edges():
            if oid in mapping:
                for ex in G[oid][sid]:
                    G[oid][sid][ex].update(source_object=oid)
            if sid in mapping:
                for ex in G[oid][sid]:
                    G[oid][sid][ex].update(source_subject=oid)
    nx.relabel_nodes(G, mapping, copy=False)

def graceful_update(a:dict, b:dict):
    """
    Updates list values appropriately. This method will not change the type of
    a value that already exists in a. If a value in a is a list, it will have
    the value of b either appened or concatenated depending on whether the value
    of b is also a list.
    """
    for key, value in b.items():
        if key in a:
            if isinstance(a[key], list) and isinstance(value, list):
                for x in value:
                    if x not in a[key]:
                        a[key].append(x)
            elif isinstance(a[key], list) and not isinstance(value, list):
                if value not in a[key]:
                    a[key].append(value)
            elif a[key] is None:
                a[key] = value
            else:
                pass
        else:
            a[key] = value


def relabel_nodes(graph:nx.Graph, mapping:dict) -> nx.Graph:
    """
    Performs the relabelling of nodes, and ensures that node attributes are
    copied over appropriately.

    Example:
        graph = nx.Graph()

        graph.add_edge('a', 'b')
        graph.add_edge('c', 'd')

        graph.node['a']['synonym'] = ['A']
        graph.node['b']['synonym'] = ['B']
        graph.node['c']['synonym'] = ['C']
        graph.node['d']['synonym'] = ['D']

        graph = relabel_nodes(graph, {'c' : 'b'})

        for n in graph.nodes():
            print(n, graph.node[n])
    Output:
        a {'synonym': ['A']}
        b {'synonym': ['B', 'C']}
        d {'synonym': ['D']}
    """
    print('relabelling nodes...')
    g = nx.relabel_nodes(graph, mapping, copy=True)

    with click.progressbar(graph.nodes(), label='merging node attributes') as bar:
        for n in bar:
            if n in mapping:
                graceful_update(g.node[mapping[n]], graph.node[n])
            elif n in g:
                graceful_update(g.node[n], graph.node[n])
            else:
                pass

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

def clique_merge(graph:nx.Graph, report=False) -> nx.Graph:
    """
    Builds up cliques using the `same_as` attribute of each node. Uses those
    cliques to build up a mapping for relabelling nodes. Chooses labels so as
    to preserve the original nodes, rather than taking xrefs that don't appear
    as nodes in the graph.

    This method will also expand the `same_as` attribute of the nodes to
    include the discovered clique.
    """
    original_size = len(graph)
    print('original graph has {} nodes'.format(original_size))

    cliqueGraph = nx.Graph()

    with click.progressbar(graph.nodes(data=True), label='building cliques from same_as node property') as bar:
        for n, attr_dict in bar:
            if 'same_as' in attr_dict:
                for m in attr_dict['same_as']:
                    cliqueGraph.add_edge(n, m)

    with click.progressbar(graph.edges(data=True), label='building cliques from same_as edges') as bar:
        for u, v, attr_dict in bar:
            if 'edge_label' in attr_dict and attr_dict['edge_label'] == 'same_as':
                cliqueGraph.add_edge(u, v)

    edges = []
    with click.progressbar(cliqueGraph.edges(), label='Breaking invalid cliques') as bar:
        for u, v in bar:
            try:
                u_categories = graph.node[u].get('category', [])
                v_categories = graph.node[v].get('category', [])
            except:
                continue
            l = len(edges)
            for a in u_categories:
                if len(edges) > l:
                    break
                for b in v_categories:
                    a_ancestors = get_toolkit().ancestors(a)
                    b_ancestors = get_toolkit().ancestors(b)
                    if a not in b_ancestors and b not in a_ancestors:
                        edges.append((u, v))
                        break

    print('breaking {} many edges'.format(len(edges)))
    cliqueGraph.remove_edges_from(edges)

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
                    list_of_prefixes.append(get_toolkit().get_element(category).id_prefixes)
                except:
                    pass

            nodes.sort()
            nodes.sort(key=build_sort_key(list_of_prefixes))

            for n in nodes:
                if n != nodes[0]:
                    mapping[n] = nodes[0]

    g = relabel_nodes(graph, mapping)

    edges = []
    for u, v, key, data in g.edges(keys=True, data=True):
        if data['edge_label'] == 'same_as':
            edges.append((u, v, key))
    g.remove_edges_from(edges)

    for n, data in g.nodes(data=True):
        data['iri'] = expand_uri(n)
        if 'id' in data and data['id'] != n:
            data['id'] = n
        if 'same_as' in data and n in data['same_as']:
            data['same_as'].remove(n)
            if data['same_as'] == []:
                del data['same_as']

    final_size = len(g)
    print('Resulting graph has {} nodes'.format(final_size))
    print('Eliminated {} nodes'.format(original_size - final_size))

    return g
