from typing import Tuple, Optional

from networkx import MultiDiGraph

from kgx.utils.kgx_utils import get_toolkit

toolkit = get_toolkit()

ignore = ['All', 'entity']

def subclasses(n, graph:MultiDiGraph):
    nodes = [n]
    while nodes != []:
        m = nodes.pop()
        for subclass, _, edge_label in graph.in_edges(m, data='edge_label'):
            if edge_label == 'subclass_of':
                nodes.append(subclass)
                yield subclass

def fill_categories(graph:MultiDiGraph) -> None:
    for n, name in graph.nodes(data='name'):
        if name is not None:
            c = toolkit.get_element(name)
            if c is not None:
                for subclass in subclasses(n):
                    if not isinstance(G.node[subclass].get('category'), list):
                        G.node[subclass]['category'] = [name]
                    else:
                        category = G.node[subclass]['category']
                        if name not in category:
                            category.append(name)

def walk(node, next_node_generator):
    to_visit = {node : 0} # Dict[str, Integer]
    visited = {} # Dict[str, Integer]

    while to_visit != {}:
        m, score = to_visit.popitem()
        visited[m] = score
        for t in next_node_generator(m):
            if isinstance(t, tuple) and len(t) > 1:
                n, s = t
            else:
                n, s = t, 0
            if n not in visited:
                to_visit[n] = score + s
                yield n, to_visit[n]

def find_superclass(node, graph:MultiDiGraph) -> Optional[str]:
    """
    Attempts to find a superclass for the given node in the given graph. Chooses
    superclasses that are in the biolink model whenever able.
    """
    def super_class_generator(n) -> Tuple[str, int]:
        if n in graph and 'type' in graph.node[n]:
            yield graph.node[n]['type']

        for _, m, data in graph.out_edges(n, data=True):
            edge_label = data.get('edge_label')
            if edge_label is None:
                continue
            elif edge_label == 'same_as':
                yield m, 0
            elif edge_label == 'subclass_of':
                yield m, 1

        for m, _, data in graph.in_edges(n, data=True):
            edge_label = data.get('edge_label')
            if edge_label is None:
                continue
            elif data['edge_label'] == 'same_as':
                yield m, 0

    best_node, best_score = None, 0

    for n, score in walk(node, super_class_generator):
        if n not in graph:
            continue

        name = graph.node[n].get('name')

        if name is not None and name not in ignore:
            c = toolkit.get_element(name)
            if c is not None and c.name is not None:
                return c.name

            if score > best_score:
                best_node, best_score = n, score

    if best_node is not None:
        return graph.node[best_node]['name']
