from typing import List
import networkx as nx

from kgx.config import get_logger
from kgx.utils.kgx_utils import prepare_data_dict


log = get_logger()


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
    log.debug(f"Largest graph {largest.name} has {len(largest.nodes())} nodes and {len(largest.edges())} edges")
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
        node_merge_count = add_all_nodes(graph, g, preserve)
        edge_merge_count = add_all_edges(graph, g, preserve)
        log.info(f"Number of nodes merged between {graph.name} and {g.name}: {node_merge_count}")
        log.info(f"Number of edges merged between {graph.name} and {g.name}: {edge_merge_count}")
    return graph


def add_all_nodes(g1: nx.MultiDiGraph, g2: nx.MultiDiGraph, preserve: bool = True) -> int:
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

    Returns
    -------
    int
        Number of nodes merged during this operation

    """
    log.info(f"Adding {g2.number_of_nodes()} nodes from {g2.name} to {g1.name}")
    merge_count = 0
    for n, data in g2.nodes(data=True):
        if n in g1.nodes():
            merge_node(g1, n, data, preserve)
            merge_count += 1
        else:
            g1.add_node(n, **data)
    return merge_count


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
    new_data = prepare_data_dict(existing_node, data, preserve)
    existing_node.update(new_data)
    return existing_node


def add_all_edges(g1: nx.MultiDiGraph, g2: nx.MultiDiGraph, preserve: bool = True) -> int:
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

    Returns
    -------
    int
        Number of edges merged during this operation

    """
    log.info(f"Adding {g2.number_of_edges()} edges from {g2} to {g1}")
    merge_count = 0
    for u, v, key, data in g2.edges(keys=True, data=True):
        if g1.has_edge(u, v, key):
            merge_edge(g1, u, v, key, data, preserve)
            merge_count += 1
        else:
            g1.add_edge(u, v, key, **data)
    return merge_count


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
    new_data = prepare_data_dict(existing_edge, data, preserve)
    existing_edge.update(new_data)
    return existing_edge
