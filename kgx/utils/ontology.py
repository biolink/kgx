from networkx import MultiDiGraph
from collections import defaultdict
from typing import List, Tuple, Optional
import bmt

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
        if 'name' in graph.node[n]:
            c = bmt.get_element(graph.node[n]['name'])
            if c is not None and c.name is not None:
                return c.name
        elif score > best_score and node.name is not None:
            best_node, best_score = node, score

    if best_node is not None:
        return graph.node[best_node].get('name')
