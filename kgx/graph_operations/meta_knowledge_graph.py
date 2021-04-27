from json import dump
from json.encoder import JSONEncoder
from typing import Dict, List, Any

from kgx.prefix_manager import PrefixManager
from kgx.graph.base_graph import BaseGraph

"""
Generate a knowledge map that corresponds to TRAPI KnowledgeMap.
Specification based on TRAPI Draft PR: https://github.com/NCATSTranslator/ReasonerAPI/pull/171

"""


def mkg_default(o):
    """
    JSONEncoder 'default' function override to
    properly serialize 'Set' objects (into 'List')
    """
    try:
        iterable = iter(o)
    except TypeError:
        pass
    else:
        return list(iterable)
    # Let the base class default method raise the TypeError
    return JSONEncoder.default(o)


def generate_knowledge_map(graph: BaseGraph, name: str, filename: str) -> None:
    """
    Generate a knowledge map that describes the composition of the graph
    and write to ``filename``.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    name: Optional[str]
        Name for the graph
    filename: str
        The file to write the knowledge map to

    """
    meta_knowledge_graph = summarize_graph(graph, name)
    WH = open(filename, 'w')
    dump(meta_knowledge_graph, WH, indent=4, default=mkg_default)


def summarize_graph(graph: BaseGraph, name: str = None, **kwargs) -> Dict:
    """
    Generate a meta knowledge graph that describes the composition of the graph.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    name: Optional[str]
        Name for the graph
    kwargs: Dict
        Any additional arguments

    Returns
    -------
    Dict
        A knowledge map dictionary corresponding to the graph

    """
    node_stats = summarize_nodes(graph)
    edge_stats = summarize_edges(graph)
    # graph_stats = {'knowledge_map': {'nodes': node_stats, 'edges': edge_stats}}
    # JSON sent back as TRAPI 1.1 version, without the global 'knowledge_map' object tag
    graph_stats = {'nodes': node_stats, 'edges': edge_stats}
    if name:
        graph_stats['name'] = name
    return graph_stats


def summarize_nodes(graph: BaseGraph) -> Dict:
    """
    Summarize the nodes in a graph.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph

    Returns
    -------
    Dict
        The node stats
    """
    node_stats: Dict[str, Dict[str, Any]] = dict()

    # The TRAPI release 1.1 meta_knowledge_graph format
    # indexes nodes by biolink:Category
    for n, data in graph.nodes(data=True):
        # the node 'category' field is a list of assigned categories (usually just one...)
        # this may result in duplicate counting and conflation of prefixes(?)
        for category in data['category']:
            if category not in node_stats:
                node_stats[category] = dict()
                node_stats[category]['id_prefixes'] = set()
                node_stats[category]['count'] = 0
                node_stats[category]['count_by_source'] = {'unknown': 0}
            prefix = PrefixManager.get_prefix(n)
            node_stats[category]['count'] += 1
            if prefix not in node_stats[category]['id_prefixes']:
                node_stats[category]['id_prefixes'].add(prefix)
            if 'provided_by' in data:
                for s in data['provided_by']:
                    if s in node_stats[category]['count_by_source']:
                        node_stats[category]['count_by_source'][s] += 1
                    else:
                        node_stats[category]['count_by_source'][s] = 1
            else:
                node_stats[category]['count_by_source']['unknown'] += 1
            
    return node_stats


def summarize_edges(graph: BaseGraph) -> List[Dict]:
    """
    Summarize the edges in a graph.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph

    Returns
    -------
    List[Dict]
        The edge stats

    """
    association_map = {}
    for u, v, k, data in graph.edges(keys=True, data=True):
        if graph.has_node(u):
            subject_node = graph.nodes()[u]
            subject_category = subject_node['category'][0]
        else:
            subject_category = 'unknown'
        if graph.has_node(v):
            object_node = graph.nodes()[v]
            object_category = object_node['category'][0]
        else:
            object_category = 'unknown'

        triple = (subject_category, data['predicate'], object_category)
        if triple not in association_map:
            association_map[triple] = {
                'subject': triple[0],
                'predicate': triple[1],
                'object': triple[2],
                'relations': set(),
                'count': 0,
                'count_by_source': {'unknown': 0},
            }

        if data['relation'] not in association_map[triple]['relations']:
            association_map[triple]['relations'].add(data['relation'])

        association_map[triple]['count'] += 1
        if 'provided_by' in data:
            for s in data['provided_by']:
                if s not in association_map[triple]['count_by_source']:
                    association_map[triple]['count_by_source'][s] = 1
                else:
                    association_map[triple]['count_by_source'][s] += 1
        else:
            association_map[triple]['count_by_source']['unknown'] += 1

    edge_stats = []
    for k, v in association_map.items():
        kedge = v
        relations = list(v['relations'])
        kedge['relations'] = relations
        edge_stats.append(kedge)
    return edge_stats
