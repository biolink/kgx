import logging
from typing import List

import networkx as nx


# TODO: refer to Biolink Model to populate this
CORE_NODE_PROPERTIES = {'id', 'name'}
CORE_EDGE_PROPERTIES = {'id', 'subject', 'edge_label', 'object', 'relation'}


def merge_all_graphs(graphs: List[nx.MultiDiGraph], preserve: bool = True) -> nx.MultiDiGraph:
    """
    Merge one or more graphs.

    .. note::
        This method will first pick the largest graph in ``graphs`` and use that
        as the target to merge the remaining graphs. This is to reduce the memory
        footprint for this operation. The criteria for largest graph is the graph
        with the largest number of edges.

        The caveat is that the merge operation has a side effect where the largest
        graph is altered.

        If you would like to ensure that all incoming graphs remain as-is, then
        look at ``merge_graphs``.

    The outcome of the merge on node and edge properties depend on the ``preserve`` parameter.
    If preserve is ``True`` then,
    - core properties will not be overwritten
    - other properties will be concatenated to a list

    If preserve is ``False`` then,
    - core properties will not be overwritten
    - other properties will be replaced

    Parameters
    ----------
    graphs: List[networkx.MultiDiGraph]
        A list of networkx.MultiDiGraph to merge
    preserve: bool
        Whether or not to preserve conflicting properties

    Returns
    -------
    nx.MultiDiGraph
        The merged graph

    """
    graph_size = [len(x.edges()) for x in graphs]
    largest = graphs.pop(graph_size.index(max(graph_size)))
    logging.debug(f"Largest graph {largest.name} has {len(largest.nodes())} nodes and {len(largest.edges())} edges")
    merged_graph = merge_graphs(largest, graphs, preserve)
    return merged_graph


def merge_graphs(graph: nx.MultiDiGraph, graphs: List[nx.MultiDiGraph], preserve: bool = True) -> nx.MultiDiGraph:
    """
    Merge all graphs in ``graphs`` to ``graph``.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        A networkx graph
    graphs: List[networkx.MultiDiGraph]
        A list of networkx.MultiDiGraph to merge
    preserve: bool
        Whether or not to preserve conflicting properties

    Returns
    -------
    nx.MultiDiGraph
        The merged graph

    """
    for g in graphs:
        add_all_nodes(graph, g, preserve)
        add_all_edges(graph, g, preserve)
    return graph


def add_all_nodes(g1: nx.MultiDiGraph, g2: nx.MultiDiGraph, preserve: bool = True) -> None:
    """
    Add all nodes from source graph (``g2``) to target graph (``g1``).

    Parameters
    ----------
    g1: networkx.MultiDiGraph
        Target graph
    g2: networkx.MultiDiGraph
        Source graph
    preserve: bool
        Whether or not to preserve conflicting properties

    """
    logging.info(f"Adding {g2.number_of_nodes()} nodes from {g2.name} to {g1.name}")
    for n, data in g2.nodes(data=True):
        if n in g1.nodes():
            merge_node(g1, n, data, preserve)
        else:
            g1.add_node(n, **data)


def merge_node(g: nx.MultiDiGraph, n: str, data: dict, preserve: bool = True) -> dict:
    """
    Merge node ``n`` into graph ``g``.

    Parameters
    ----------
    g: nx.MultiDiGraph
        The target graph
    n: str
        Node id
    data: dict
        Node properties
    preserve: bool
        Whether or not to preserve conflicting properties

    Returns
    -------
    dict
        The merged node

    """
    existing_node = g.nodes[n]
    for k, v in data.items():
        if k in existing_node:
            if k in CORE_NODE_PROPERTIES:
                logging.debug(f"cannot modify core node property '{k}': {existing_node[k]} vs {v}")
            elif k == 'category':
                logging.debug(f"updating node property '{k}'; Appending {v} to {existing_node[k]}")
                if isinstance(v, list):
                    existing_node[k].extend(v)
                else:
                    existing_node[k].append(v)
                existing_node['category'] = list(set(existing_node[k]))
            else:
                if isinstance(existing_node[k], list):
                    # append
                    logging.debug(f"node property '{k}' is a list; Appending {v} to {existing_node[k]}")
                    if isinstance(v, list):
                        existing_node[k].extend(v)
                    else:
                        existing_node[k].append(v)
                else:
                    if preserve:
                        # convert to a list and append
                        logging.debug(f"preserving node property '{k}'; Appending {v} to {existing_node[k]}")
                        existing_node[k] = [existing_node[k]]
                        existing_node[k].append(v)
                    else:
                        # overwrite the value for key
                        logging.debug(f"overwriting node property '{k}'; Replacing {existing_node[k]} with {v}")
                        existing_node[k] = v
        else:
            logging.debug(f"adding new node property {k} to node")
            existing_node[k] = v
    return existing_node


def add_all_edges(g1: nx.MultiDiGraph, g2: nx.MultiDiGraph, preserve: bool = True) -> None:
    """
    Add all edges from source graph (``g2``) to target graph (``g1``).

    Parameters
    ----------
    g1: networkx.MultiDiGraph
        Target graph
    g2: networkx.MultiDiGraph
        Source graph
    preserve: bool
        Whether or not to preserve conflicting properties

    """
    logging.info(f"Adding {g2.number_of_edges()} edges from {g2} to {g1}")
    for u, v, key, data in g2.edges(keys=True, data=True):
        if g1.has_edge(u, v, key):
            merge_edge(g1, u, v, key, data, preserve)
        else:
            g1.add_edge(u, v, key, **data)


def merge_edge(g: nx.MultiDiGraph, u: str, v: str, key: str, data: dict, preserve: bool = True) -> dict:
    """
    Merge edge ``u`` -> ``v`` into graph ``g``.

    Parameters
    ----------
    g: nx.MultiDiGraph
        The target graph
    u: str
        Subject node id
    v: str
        Object node id
    key: str
        Edge key
    data: dict
        Node properties
    preserve: bool
        Whether or not to preserve conflicting properties

    Returns
    -------
    dict
        The merged edge

    """
    existing_edge = g.get_edge_data(u, v, key)
    for k, v in data.items():
        if k in existing_edge:
            if k in CORE_EDGE_PROPERTIES:
                logging.debug(f"cannot modify core edge property '{k}': {existing_edge[k]} vs {v}")
            else:
                if isinstance(existing_edge[k], list):
                    # append
                    logging.debug(f"edge property '{k}' list a list; Appending {v} to {existing_edge[k]}")
                    if isinstance(v, list):
                        existing_edge[k].extend(v)
                    else:
                        existing_edge[k].append(v)
                else:
                    if preserve:
                        # convert to a list and append
                        logging.debug(f"preserving edge property '{k}'; Appending {v} to {existing_edge[k]}")
                        existing_edge[k] = [existing_edge[k]]
                        existing_edge[k].append(v)
                    else:
                        # overwrite the value for key
                        logging.debug(f"overwriting edge property '{k}'; Replacing {existing_edge[k]} with {v}")
                        existing_edge[k] = v
        else:
            logging.debug(f"adding new edge property {k} to edge")
            existing_edge[k] = v
    return existing_edge
