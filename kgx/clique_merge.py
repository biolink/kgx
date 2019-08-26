import logging

import networkx as nx
from kgx.utils.kgx_utils import generate_edge_key

SAME_AS = 'same_as'
LEADER_ANNOTATION = 'clique_leader'
PREFIX_PRIORITIZATION_MAP = {
    'gene': ['HGNC', 'NCBIGene', 'Ensembl']
}


def build_cliques(target_graph: nx.MultiDiGraph) -> nx.Graph:
    """
    Builds a clique graph from same_as edges in target_graph.

    Parameters
    ----------
    target_graph: networkx.MultiDiGraph
        A MultiDiGraph that contains nodes and edges

    Returns
    -------
    networkx.Graph
        The clique graph with only same_as edges

    """
    clique_graph = nx.Graph()
    for u, v, data in target_graph.edges(data=True):
        if 'edge_label' in data and data['edge_label'] == SAME_AS:
            # load all same_as edges to clique_graph
            clique_graph.add_edge(u, v, **data)
    return clique_graph


def elect_leader(clique_graph: nx.Graph) -> nx.Graph:
    """
    Elect leader for each clique in a graph.

    Parameters
    ----------
    clique_graph: networkx.Graph
        The clique graph

    Returns
    -------
    networkx.Graph
        The clique graph where each clique has a leader

    """
    cliques = list(nx.connected_components(clique_graph))
    election_strategy = None
    for clique in cliques:
        leader = None
        logging.info("processing clique: {}".format(clique))
        for node in clique:
            node_attributes = clique_graph.node[node]
            if LEADER_ANNOTATION in node_attributes and eval(node_attributes[LEADER_ANNOTATION]):
                logging.info("node {} in clique has LEADER_ANNOTATION property; electing it as clique leader".format(node))
                election_strategy = 'LEADER_ANNOTATION'

        if leader is None:
            logging.info("Could not elect clique leader by looking for LEADER_ANNOTATION property; Using prefix prioritization instead")
            # assuming nodes in a clique will be of the same category
            # and assuming category is 'gene'
            # TODO
            category = 'gene'
            leader = get_node_by_prefix_priority(clique, PREFIX_PRIORITIZATION_MAP[category])
            election_strategy = 'PREFIX_PRIORITIZATION'

            if leader is None:
                logging.info("Could not elect clique leader by PREFIX_PRIORITIZATION")
                clique_list = list(clique)
                clique_list.sort()
                leader = clique_list[0]
                election_strategy = 'ALPHABETICAL SORT'
                logging.info("Picking node {} from an alphabetically sorted list".format(leader))

        clique_graph.node[leader][LEADER_ANNOTATION] = True
        clique_graph.node[leader]['election_strategy'] = election_strategy

    return clique_graph


def get_node_by_prefix_priority(clique: list, prefix_priority_list: list) -> str:
    """
    Get node from clique based on a given prefix priority.

    Parameters
    ----------
    clique: list
        A list of nodes that correspond to a clique
    prefix_priority_list: list
        A list of prefixes in descending priority

    Returns
    -------
    str
        A node

    """
    leader = None
    for prefix in prefix_priority_list:
        logging.info("Checking for prefix {} in {}".format(prefix, clique))
        leader = next((s for s in clique if prefix in s), None)
        if leader:
            break
    return leader


def consolidate_edges(target_graph: nx.MultiDiGraph, clique_graph: nx.Graph) -> nx.MultiDiGraph:
    """
    Move all edges from nodes in a clique to clique_leader.

    Parameters
    ----------
    target_graph: networkx.MultiDiGraph
        A MultiDiGraph that contains nodes and edges
    clique_graph: networkx.Graph
        A Graph that with cliques where each clique has one elected leader

    Returns
    -------
    nx.MultiDiGraph
        The target graph where all edges from nodes in a clique are moved to clique leader

    """
    cliques = list(nx.connected_components(clique_graph))
    for clique in cliques:
        leader = [x for x in clique if LEADER_ANNOTATION in clique_graph.node[x] and clique_graph.node[x][LEADER_ANNOTATION]]
        if len(leader) == 0:
            logging.info("No leader for clique {}; skipping".format(clique))
            continue
        else:
            leader = leader[0]
        nx.set_node_attributes(target_graph, {leader: {LEADER_ANNOTATION: clique_graph.node[leader].get(LEADER_ANNOTATION)}})
        for node in clique:
            if node == leader:
                continue
            in_edges = [x for x in target_graph.in_edges(node, True) if x[2]['edge_label'] != SAME_AS]
            logging.info("Moving {} in-edges from {} to {}".format(len(in_edges), node, leader))
            for u, v, edge_data in in_edges:
                key = generate_edge_key(u, edge_data['edge_label'], v)
                target_graph.remove_edge(u, v, key=key)
                edge_data['_original_subject'] = edge_data['subject']
                edge_data['_original_object'] = edge_data['object']
                edge_data['object'] = leader
                key = generate_edge_key(u, edge_data['edge_label'], v)
                target_graph.add_edge(edge_data['subject'], edge_data['object'], key, **edge_data)

            out_edges = [x for x in target_graph.out_edges(node, True) if x[2]['edge_label'] != SAME_AS]
            logging.info("Moving {} out-edges from {} to {}".format(len(out_edges), node, leader))
            for u, v, edge_data in out_edges:
                key = generate_edge_key(u, edge_data['edge_label'], v)
                target_graph.remove_edge(u, v, key=key)
                edge_data['_original_subject'] = edge_data['subject']
                edge_data['_original_object'] = edge_data['object']
                edge_data['subject'] = leader
                key = generate_edge_key(u, edge_data['edge_label'], v)
                target_graph.add_edge(edge_data['subject'], edge_data['object'], key, **edge_data)

    return target_graph

