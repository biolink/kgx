"""
TODO: add methods for ensuring that other biolink model specifications hold,
like that all required properties are present and that they have the correct
multiplicity, and that all identifiers are CURIE's.
"""

import networkx as nx
import bmt

def make_valid_types(G:nx.MultiDiGraph) -> None:
    """
    Ensures that all the nodes have valid categories, and that all edges have
    valid edge labels.

    Nodes will be deleted if they have no name and have no valid categories. If
    a node has no valid category but does have a name then its category will be
    set to the default category "named thing".

    Edges with invalid edge labels will have their edge label set to the default
    value "related_to"
    """
    nodes = []

    for n, data in G.nodes(data=True):
        data['category'] = [c for c in data.get('category', []) if bmt.get_class(c) is not None]
        if data['category'] == []:
            if 'name' in data:
                data['category'] = ['named thing']
            else:
                nodes.append(n)

    G.remove_nodes_from(nodes)

    for u, v, data in G.edges(data=True):
        if bmt.get_predicate(data['edge_label']) is None:
            data['edge_label'] = 'related_to'
        elif ' ' in data['edge_label']:
            data['edge_label'] = data['edge_label'].replace(' ', '_')
