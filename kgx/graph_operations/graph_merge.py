import copy
from typing import List

from kgx.config import get_logger
from kgx.graph.base_graph import BaseGraph
from kgx.utils.kgx_utils import prepare_data_dict


log = get_logger()


def merge_all_graphs(graphs: List[BaseGraph], preserve: bool = True) -> BaseGraph:
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
    graphs: List[kgx.graph.base_graph.BaseGraph]
        A list of instances of BaseGraph to merge
    preserve: bool
        Whether or not to preserve conflicting properties

    Returns
    -------
    kgx.graph.base_graph.BaseGraph
        The merged graph

    """
    graph_size = [len(x.edges()) for x in graphs]
    largest = graphs.pop(graph_size.index(max(graph_size)))
    log.debug(
        f"Largest graph {largest.name} has {len(largest.nodes())} nodes and {len(largest.edges())} edges"
    )
    merged_graph = merge_graphs(largest, graphs, preserve)
    return merged_graph


def merge_graphs(
    graph: BaseGraph, graphs: List[BaseGraph], preserve: bool = True
) -> BaseGraph:
    """
    Merge all graphs in ``graphs`` to ``graph``.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        An instance of BaseGraph
    graphs: List[kgx.graph.base_graph.BaseGraph]
        A list of instances of BaseGraph to merge
    preserve: bool
        Whether or not to preserve conflicting properties

    Returns
    -------
    kgx.graph.base_graph.BaseGraph
        The merged graph

    """
    for g in graphs:
        node_merge_count = add_all_nodes(graph, g, preserve)
        log.info(
            f"Number of nodes merged between {graph.name} and {g.name}: {node_merge_count}"
        )
        edge_merge_count = add_all_edges(graph, g, preserve)
        log.info(
            f"Number of edges merged between {graph.name} and {g.name}: {edge_merge_count}"
        )
    return graph


def add_all_nodes(g1: BaseGraph, g2: BaseGraph, preserve: bool = True) -> int:
    """
    Add all nodes from source graph (``g2``) to target graph (``g1``).

    Parameters
    ----------
    g1: kgx.graph.base_graph.BaseGraph
        Target graph
    g2: kgx.graph.base_graph.BaseGraph
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


def merge_node(g: BaseGraph, n: str, data: dict, preserve: bool = True) -> dict:
    """
    Merge node ``n`` into graph ``g``.

    Parameters
    ----------
    g: kgx.graph.base_graph.BaseGraph
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
    existing_node = g.nodes()[n]
    new_data = prepare_data_dict(
        copy.deepcopy(existing_node), copy.deepcopy(data), preserve
    )
    g.add_node(n, **new_data)
    return existing_node


def add_all_edges(g1: BaseGraph, g2: BaseGraph, preserve: bool = True) -> int:
    """
    Add all edges from source graph (``g2``) to target graph (``g1``).

    Parameters
    ----------
    g1: kgx.graph.base_graph.BaseGraph
        Target graph
    g2: kgx.graph.base_graph.BaseGraph
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
            g1.add_edge(u, v, edge_key=key, **data)
    return merge_count


def merge_edge(
    g: BaseGraph, u: str, v: str, key: str, data: dict, preserve: bool = True
) -> dict:
    """
    Merge edge ``u`` -> ``v`` into graph ``g``.

    Parameters
    ----------
    g: kgx.graph.base_graph.BaseGraph
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
    existing_edge = g.get_edge(u, v, key)
    new_data = prepare_data_dict(
        copy.deepcopy(existing_edge), copy.deepcopy(data), preserve
    )
    g.add_edge(u, v, edge_key=key, **new_data)
    return existing_edge
