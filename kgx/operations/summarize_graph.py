from typing import Dict

import networkx as nx
import yaml

TOTAL_NODES = 'total_nodes'
NODE_CATEGORIES = 'node_categories'
COUNT_BY_CATEGORY = 'count_by_category'

TOTAL_EDGES = 'total_edges'
EDGE_LABELS = 'edge_labels'
COUNT_BY_EDGE_LABEL = 'count_by_edge_label'
COUNT_BY_SPO = 'count_by_spo'

# Note: the format of the stats generated might change in the future

def generate_graph_stats(graph: nx.MultiDiGraph, graph_name: str, filename: str) -> None:
    """
    Generate stats from Graph.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        The graph
    graph_name: str
        Name for the graph
    filename: str
        Filename to write the stats to

    """
    stats = summarize_graph(graph, graph_name)
    WH = open(filename, 'w')
    yaml.dump(stats, WH)

def summarize_graph(graph: nx.MultiDiGraph, name: str = None) -> Dict:
    """
    Summarize the entire graph.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        The graph
    graph_name: str
        Name for the graph

    Returns
    -------
    Dict
        The stats dictionary

    """
    node_stats = summarize_nodes(graph)
    edge_stats = summarize_edges(graph)
    stats = {
        'graph_name': name if name else graph.name,
        'node_stats': node_stats,
        'edge_stats': edge_stats
    }
    return stats

def summarize_nodes(graph: nx.MultiDiGraph) -> Dict:
    """
    Summarize the nodes in a graph.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        The graph

    Returns
    -------
    Dict
        The node stats

    """
    stats = {
        TOTAL_NODES: 0,
        NODE_CATEGORIES: set(),
        COUNT_BY_CATEGORY: {'unknown': 0}
    }

    stats[TOTAL_NODES] = len(graph.nodes())
    for n, data in graph.nodes(data=True):
        if 'category' not in data:
            stats[COUNT_BY_CATEGORY]['unknown'] += 1
            continue
        categories = data['category']
        stats[NODE_CATEGORIES].update(categories)
        for category in categories:
            if category in stats[COUNT_BY_CATEGORY]:
                stats[COUNT_BY_CATEGORY][category] += 1
            else:
                stats[COUNT_BY_CATEGORY][category] = 1
    stats[NODE_CATEGORIES] = list(stats[NODE_CATEGORIES])
    return stats

def summarize_edges(graph: nx.MultiDiGraph):
    """
    Summarize the edges in a graph.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        The graph

    Returns
    -------
    Dict
        The edge stats

    """
    stats = {
        TOTAL_EDGES: 0,
        EDGE_LABELS: set(),
        COUNT_BY_EDGE_LABEL: {'unknown': 0},
        COUNT_BY_SPO: {}
    }

    stats[TOTAL_EDGES] = len(graph.edges())
    for u, v, k, data in graph.edges(keys=True, data=True):
        if 'edge_label' not in data:
            stats[COUNT_BY_EDGE_LABEL]['unknown'] += 1
            edge_label = "unknown"
        else:
            edge_label = data['edge_label']
            stats[EDGE_LABELS].add(edge_label)
            if edge_label in stats[COUNT_BY_EDGE_LABEL]:
                stats[COUNT_BY_EDGE_LABEL][edge_label] += 1
            else:
                stats[COUNT_BY_EDGE_LABEL][edge_label] = 1

        u_data = graph.nodes[u]
        v_data = graph.nodes[v]

        if 'category' in u_data:
            u_category = u_data['category'][0]
        else:
            u_category = "unknown"

        if 'category' in v_data:
            v_category = v_data['category'][0]
        else:
            v_category = "unknown"

        key = f"{u_category}-{edge_label}-{v_category}"
        if key in stats[COUNT_BY_SPO]:
            stats[COUNT_BY_SPO][key] += 1
        else:
            stats[COUNT_BY_SPO][key] = 1
    stats[EDGE_LABELS] = list(stats[EDGE_LABELS])
    return stats