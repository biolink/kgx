import json
import pprint
from typing import Dict, List, Optional

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


def generate_graph_stats(graph: nx.MultiDiGraph, graph_name: str, filename: str, node_facet_properties: Optional[List] = None, edge_facet_properties: Optional[List] = None) -> None:
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
    node_facet_properties: Optional[List]
        A list of properties to facet on. For example, ``['provided_by']``
    edge_facet_properties: Optional[List]
        A list of properties to facet on. For example, ``['provided_by']``

    """
    stats = summarize_graph(graph, graph_name, node_facet_properties, edge_facet_properties)
    WH = open(filename, 'w')
    yaml.dump(stats, WH)


def summarize_graph(graph: nx.MultiDiGraph, name: str = None, node_facet_properties: Optional[List] = None, edge_facet_properties: Optional[List] = None) -> Dict:
    """
    Summarize the entire graph.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        The graph
    name: str
        Name for the graph
    node_facet_properties: Optional[List]
        A list of properties to facet on. For example, ``['provided_by']``
    edge_facet_properties: Optional[List]
        A list of properties to facet on. For example, ``['provided_by']``

    Returns
    -------
    Dict
        The stats dictionary

    """
    node_stats = summarize_nodes(graph, node_facet_properties)
    edge_stats = summarize_edges(graph, edge_facet_properties)
    stats = {
        'graph_name': name if name else graph.name,
        'node_stats': node_stats,
        'edge_stats': edge_stats
    }
    return stats


def summarize_nodes(graph: nx.MultiDiGraph, facet_properties: Optional[List] = None) -> Dict:
    """
    Summarize the nodes in a graph.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        The graph
    facet_properties: Optional[List]
        A list of properties to facet on

    Returns
    -------
    Dict
        The node stats

    """
    stats: Dict = {
        TOTAL_NODES: 0,
        NODE_CATEGORIES: set(),
        COUNT_BY_CATEGORY: {'unknown': {'count': 0}}
    }

    stats[TOTAL_NODES] = len(graph.nodes())
    if facet_properties:
        for facet_property in facet_properties:
            stats[facet_property] = set()

    for n, data in graph.nodes(data=True):
        if 'category' not in data:
            stats[COUNT_BY_CATEGORY]['unknown']['count'] += 1
            continue
        categories = data['category']
        stats[NODE_CATEGORIES].update(categories)
        for category in categories:
            if category in stats[COUNT_BY_CATEGORY]:
                stats[COUNT_BY_CATEGORY][category]['count'] += 1
            else:
                stats[COUNT_BY_CATEGORY][category] = {'count': 1}

            if facet_properties:
                for facet_property in facet_properties:
                    stats = get_facet_counts(data, stats, COUNT_BY_CATEGORY, category, facet_property)

    stats[NODE_CATEGORIES] = sorted(list(stats[NODE_CATEGORIES]))
    if facet_properties:
        for facet_property in facet_properties:
            stats[facet_property] = sorted(list(stats[facet_property]))
    return stats


def summarize_edges(graph: nx.MultiDiGraph, facet_properties: Optional[List] = None):
    """
    Summarize the edges in a graph.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        The graph
    facet_properties: Optional[List]
        The properties to facet on

    Returns
    -------
    Dict
        The edge stats

    """
    stats: Dict = {
        TOTAL_EDGES: 0,
        EDGE_LABELS: set(),
        COUNT_BY_EDGE_LABEL: {'unknown': {'count': 0}},
        COUNT_BY_SPO: {}
    }

    stats[TOTAL_EDGES] = len(graph.edges())
    if facet_properties:
        for facet_property in facet_properties:
            stats[facet_property] = set()

    for u, v, k, data in graph.edges(keys=True, data=True):
        if 'edge_label' not in data:
            stats[COUNT_BY_EDGE_LABEL]['unknown']['count'] += 1
            edge_label = "unknown"
        else:
            edge_label = data['edge_label']
            stats[EDGE_LABELS].add(edge_label)
            if edge_label in stats[COUNT_BY_EDGE_LABEL]:
                stats[COUNT_BY_EDGE_LABEL][edge_label]['count'] += 1
            else:
                stats[COUNT_BY_EDGE_LABEL][edge_label] = {'count': 1}

            if facet_properties:
                for facet_property in facet_properties:
                    stats = get_facet_counts(data, stats, COUNT_BY_EDGE_LABEL, edge_label, facet_property)

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
            stats[COUNT_BY_SPO][key]['count'] += 1
        else:
            stats[COUNT_BY_SPO][key] = {'count': 1}

        if facet_properties:
            for facet_property in facet_properties:
                stats = get_facet_counts(data, stats, COUNT_BY_SPO, key, facet_property)

    stats[EDGE_LABELS] = sorted(list(stats[EDGE_LABELS]))
    if facet_properties:
        for facet_property in facet_properties:
            stats[facet_property] = sorted(list(stats[facet_property]))

    return stats


def get_facet_counts(data: Dict, stats: Dict, x: str, y: str, facet_property: str) -> Dict:
    """
    Facet on ``facet_property`` and record the count for ``stats[x][y][facet_property]``.

    Parameters
    ----------
    data: dict
        Node/edge data dictionary
    stats: dict
        The stats dictionary
    x: str
        first key
    y: str
        second key
    facet_property: str
        The property to facet on

    Returns
    -------
    Dict
        The stats dictionary

    """
    if facet_property in data:
        if isinstance(data[facet_property], list):
            for k in data[facet_property]:
                if facet_property not in stats[x][y]:
                    stats[x][y][facet_property] = {}

                if k in stats[x][y][facet_property]:
                    stats[x][y][facet_property][k]['count'] += 1
                else:
                    stats[x][y][facet_property][k] = {'count': 1}
                    stats[facet_property].update([k])
        else:
            k = data[facet_property]
            if facet_property not in stats[x][y]:
                stats[x][y][facet_property] = {}

            if k in stats[x][y][facet_property]:
                stats[x][y][facet_property][k]['count'] += 1
            else:
                stats[x][y][facet_property][k] = {'count': 1}
                stats[facet_property].update([k])
    else:
        if facet_property not in stats[x][y]:
            stats[x][y][facet_property] = {}
        if 'unknown' in stats[x][y][facet_property]:
            stats[x][y][facet_property]['unknown']['count'] += 1
        else:
            stats[x][y][facet_property]['unknown'] = {'count': 1}
            stats[facet_property].update(['unknown'])
    return stats
