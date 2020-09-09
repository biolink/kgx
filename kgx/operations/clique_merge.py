from typing import Tuple, Optional, Dict, List, Any, Set

import networkx as nx

from kgx.config import get_logger
from kgx.utils.kgx_utils import get_prefix_prioritization_map, get_biolink_element, get_biolink_ancestors, \
    current_time_in_millis, format_biolink_category, generate_edge_key

log = get_logger()

SAME_AS = 'biolink:same_as'
LEADER_ANNOTATION = 'cliqueLeader'
ORIGINAL_SUBJECT_PROPERTY = '_original_subject'
ORIGINAL_OBJECT_PROPERTY = '_original_object'


def clique_merge(target_graph: nx.MultiDiGraph, leader_annotation: str = None, prefix_prioritization_map: Optional[Dict[str, List[str]]] = None, category_mapping: Optional[Dict[str, str]] = None) -> Tuple[nx.MultiDiGraph, nx.Graph]:
    """

    Parameters
    ----------
    target_graph: networkx.MultiDiGraph
        The original graph
    leader_annotation: str
        The field on a node that signifies that the node is the leader of a clique
    prefix_prioritization_map: Optional[Dict[str, List[str]]]
        A map that gives a prefix priority for one or more categories
    category_mapping: Optional[Dict[str, str]]
        Mapping for non-Biolink Model categories to Biolink Model categories

    Returns
    -------
    Tuple[networkx.MultiDiGraph, networkx.Graph]
        A tuple containing the updated target graph, and the clique graph

    """
    ppm = get_prefix_prioritization_map()
    if prefix_prioritization_map:
        ppm.update(prefix_prioritization_map)
    prefix_prioritization_map = ppm

    if not leader_annotation:
        leader_annotation = LEADER_ANNOTATION

    start = current_time_in_millis()
    clique_graph = build_cliques(target_graph)
    end = current_time_in_millis()
    log.info(f"Total time taken to build cliques: {end - start} ms")

    start = current_time_in_millis()
    elect_leader(target_graph, clique_graph, leader_annotation, prefix_prioritization_map, category_mapping)
    end = current_time_in_millis()
    log.info(f"Total time taken to elect leaders for all cliques: {end - start} ms")

    start = current_time_in_millis()
    graph = consolidate_edges(target_graph, clique_graph, leader_annotation)
    end = current_time_in_millis()
    log.info(f"Total time taken to consolidate edges in target graph: {end - start} ms")
    return graph, clique_graph


def build_cliques(target_graph: nx.MultiDiGraph) -> nx.Graph:
    """
    Builds a clique graph from ``same_as`` edges in ``target_graph``.

    Parameters
    ----------
    target_graph: networkx.MultiDiGraph
        A MultiDiGraph that contains nodes and edges

    Returns
    -------
    networkx.Graph
        The clique graph with only ``same_as`` edges

    """
    clique_graph = nx.Graph()
    for u, v, data in target_graph.edges(data=True):
        if 'edge_label' in data and data['edge_label'] == SAME_AS:
            # load all biolink:same_as edges to clique_graph
            clique_graph.add_node(u, **target_graph.nodes[u])
            clique_graph.add_node(v, **target_graph.nodes[v])
            clique_graph.add_edge(u, v, **data)
    return clique_graph


def elect_leader(target_graph: nx.MultiDiGraph, clique_graph: nx.Graph, leader_annotation: str, prefix_prioritization_map: Optional[Dict[str, List[str]]], category_mapping: Optional[Dict[str, str]]) -> nx.MultiDiGraph:
    """
    Elect leader for each clique in a graph.

    Parameters
    ----------
    target_graph: networkx.MultiDiGraph
        The original graph
    clique_graph: networkx.Graph
        The clique graph
    leader_annotation: str
        The field on a node that signifies that the node is the leader of a clique
    prefix_prioritization_map: Optional[Dict[str, List[str]]]
        A map that gives a prefix priority for one or more categories
    category_mapping: Optional[Dict[str, str]]
        Mapping for non-Biolink Model categories to Biolink Model categories

    Returns
    -------
    networkx.MultiDiGraph
        The updated target graph

    """
    cliques = list(nx.connected_components(clique_graph))
    log.info(f"Total cliques in clique graph: {len(cliques)}")
    election_strategy = None
    count = 0
    for clique in cliques:
        clique_category = None
        log.debug(f"Processing clique: {clique}")
        # first update all categories for nodes in a clique
        update_node_categories(target_graph, clique_graph, clique, category_mapping)
        # validate categories of all nodes in a clique while removing
        # the nodes that are not supposed to be in the clique
        (clique_category, invalid_nodes) = validate_clique_category(target_graph, clique_graph, clique)
        log.debug(f"clique_category: {clique_category} invalid_nodes: {invalid_nodes}")
        if invalid_nodes:
            log.debug(f"Removing nodes {invalid_nodes} as they are not supposed to be part of clique")
            clique = [x for x in clique if x not in invalid_nodes]
            for n in invalid_nodes:
                # we are removing the invalid node and incoming and outgoing same_as edges
                # from this node in the clique graph and not the target graph
                clique_graph.remove_node(n)

        if clique_category:
            # First check for LEADER_ANNOTATION property
            (leader, election_strategy) = get_leader_by_annotation(target_graph, clique_graph, clique, leader_annotation)
            if leader is None:
                # If leader is None, then use prefix prioritization
                log.debug("Could not elect clique leader by looking for LEADER_ANNOTATION property; Using prefix prioritization instead")
                if prefix_prioritization_map and clique_category in prefix_prioritization_map.keys():
                    (leader, election_strategy) = get_leader_by_prefix_priority(target_graph, clique_graph, clique, prefix_prioritization_map[clique_category])
                else:
                    log.debug(f"No prefix order found for category '{clique_category}' in PREFIX_PRIORITIZATION_MAP")

            if leader is None:
                # If leader is still None then fall back to alphabetical sort on prefixes
                log.debug("Could not elect clique leader by PREFIX_PRIORITIZATION; Using alphabetical sort on prefixes")
                (leader, election_strategy) = get_leader_by_sort(target_graph, clique_graph, clique)

            log.debug(f"Elected {leader} as leader via {election_strategy} for clique {clique}")
            clique_graph.nodes[leader][LEADER_ANNOTATION] = True
            target_graph.nodes[leader][LEADER_ANNOTATION] = True
            clique_graph.nodes[leader]['election_strategy'] = election_strategy
            target_graph.nodes[leader]['election_strategy'] = election_strategy
            count += 1
    log.info(f"Total merged cliques: {count}")
    return target_graph


def consolidate_edges(target_graph: nx.MultiDiGraph, clique_graph: nx.Graph, leader_annotation: str) -> nx.MultiDiGraph:
    """
    Move all edges from nodes in a clique to the clique leader.

    Original subject and object of a node are preserved via ``ORIGINAL_SUBJECT_PROPERTY`` and ``ORIGINAL_OBJECT_PROPERTY``

    Parameters
    ----------
    target_graph: networkx.MultiDiGraph
        The original graph
    clique_graph: networkx.Graph
        The clique graph
    leader_annotation: str
        The field on a node that signifies that the node is the leader of a clique

    Returns
    -------
    nx.MultiDiGraph
        The target graph where all edges from nodes in a clique are moved to clique leader

    """
    cliques = list(nx.connected_components(clique_graph))
    log.info(f"Consolidating edges in {len(cliques)} cliques")
    for clique in cliques:
        leaders: List = [x for x in clique if leader_annotation in clique_graph.nodes[x] and clique_graph.nodes[x][leader_annotation]]
        if len(leaders) == 0:
            log.debug("No leader elected for clique {}; skipping".format(clique))
            continue
        leader: str = leaders[0]
        # update nodes in target graph
        nx.set_node_attributes(target_graph, {leader: {leader_annotation: clique_graph.nodes[leader].get(leader_annotation), 'election_strategy': clique_graph.nodes[leader].get('election_strategy')}})
        for node in clique:
            if node == leader:
                continue
            in_edges = target_graph.in_edges(node, True)
            filtered_in_edges = [x for x in in_edges if x[2]['edge_label'] != SAME_AS]
            equiv_in_edges = [x for x in in_edges if x[2]['edge_label'] == SAME_AS]
            log.debug(f"Moving {len(in_edges)} in-edges from {node} to {leader}")
            for u, v, edge_data in filtered_in_edges:
                key = generate_edge_key(u, edge_data['edge_label'], v)
                target_graph.remove_edge(u, v, key=key)
                edge_data[ORIGINAL_SUBJECT_PROPERTY] = edge_data['subject']
                edge_data[ORIGINAL_OBJECT_PROPERTY] = edge_data['object']
                edge_data['object'] = leader
                key = generate_edge_key(u, edge_data['edge_label'], leader)
                target_graph.add_edge(edge_data['subject'], edge_data['object'], key, **edge_data)

            out_edges = target_graph.out_edges(node, True)
            filtered_out_edges = [x for x in out_edges if x[2]['edge_label'] != SAME_AS]
            equiv_out_edges = [x for x in out_edges if x[2]['edge_label'] == SAME_AS]
            log.debug(f"Moving {len(out_edges)} out-edges from {node} to {leader}")
            for u, v, edge_data in filtered_out_edges:
                key = generate_edge_key(u, edge_data['edge_label'], v)
                target_graph.remove_edge(u, v, key=key)
                edge_data[ORIGINAL_SUBJECT_PROPERTY] = edge_data['subject']
                edge_data[ORIGINAL_OBJECT_PROPERTY] = edge_data['object']
                edge_data['subject'] = leader
                key = generate_edge_key(leader, edge_data['edge_label'], v)
                target_graph.add_edge(edge_data['subject'], edge_data['object'], key, **edge_data)

            equivalent_identifiers = set(target_graph.nodes[leader].get('same_as')) if 'same_as' in target_graph.nodes[leader] else set()
            for u, v, edge_data in equiv_in_edges:
                if u != leader:
                    equivalent_identifiers.add(u)
                if v != leader:
                    equivalent_identifiers.add(v)
                target_graph.remove_edge(u, v, key=generate_edge_key(u, SAME_AS, v))

            log.debug(f"equiv out edges: {equiv_out_edges}")
            for u, v, edge_data in equiv_out_edges:
                if u != leader:
                    log.debug(f"{u} is an equivalent identifier of leader {leader}")
                    equivalent_identifiers.add(u)
                if v != leader:
                    log.debug(f"{v} is an equivalent identifier of leader {leader}")
                    equivalent_identifiers.add(v)
                target_graph.remove_edge(u, v, key=generate_edge_key(u, SAME_AS, v))

            # set same_as property for leader
            nx.set_node_attributes(target_graph, {leader: {'same_as': list(equivalent_identifiers)}})
            # remove all node instances of aliases
            target_graph.remove_nodes_from(equivalent_identifiers)
    return target_graph


def update_node_categories(target_graph: nx.MultiDiGraph, clique_graph: nx.Graph, clique: List, category_mapping: Optional[Dict[str, str]]) -> List:
    """
    For a given clique, get category for each node in clique and validate against Biolink Model,
    mapping to Biolink Model category where needed.

    For example, If a node has ``biolink:Gene`` as its category, then this method adds all of its ancestors.

    Parameters
    ----------
    target_graph: networkx.MultiDiGraph
        The original graph
    clique_graph: networkx.Graph
        The clique graph
    clique: List
        A list of nodes from a clique
    category_mapping: Optional[Dict[str, str]]
        Mapping for non-Biolink Model categories to Biolink Model categories

    Returns
    -------
    List
        The clique

    """
    if not category_mapping:
        category_mapping = {}
    updated_node_categories = {}
    for node in clique:
        data = clique_graph.nodes[node]
        if 'category' in data:
            categories = data['category']
        else:
            # get category from equivalence
            categories = get_category_from_equivalence(target_graph, clique_graph, node, data)

        extended_categories: Set = set()
        invalid_categories: List = []
        for category in categories:
            log.debug(f"Looking at category: {category}")
            element = get_biolink_element(category)
            # TODO: if element is None then should also check for mapping to biolink categories; useful when category holds a value from other ontologies/CVs
            if element:
                # category exists in Biolink Model as a class or as an alias to a class
                mapped_category = element['name']
                # TODO: Cache this and see how it affects performance
                ancestors = get_biolink_ancestors(mapped_category)
                if len(ancestors) > len(extended_categories):
                    # the category with the longest list of ancestors will be the most specific category
                    extended_categories = ancestors
            else:
                log.warning(f"category '{category}' not in Biolink Model")
                invalid_categories.append(category)
        log.debug("Invalid categories: {}".format(invalid_categories))

        for x in categories:
            element = get_biolink_element(x)
            if element:
                mapped_category = format_biolink_category(element['name'])
                if mapped_category not in extended_categories:
                    log.warning(f"category '{mapped_category}' not in ancestor closure: {extended_categories}")
                    mapped = category_mapping[x] if x in category_mapping.keys() else x
                    if mapped not in extended_categories:
                        log.warning(f"category '{mapped_category}' is not even in any custom defined mapping. ")
                        invalid_categories.append(x)
            else:
                log.warning(f"category '{x}' is not in Biolink Model")
                continue
        update_dict: Dict = {'category': list(extended_categories)}
        if invalid_categories:
            update_dict['_invalid_category'] = invalid_categories
        updated_node_categories[node] = update_dict

    nx.set_node_attributes(clique_graph, updated_node_categories)
    nx.set_node_attributes(target_graph, updated_node_categories)
    return clique


def get_category_from_equivalence(target_graph: nx.MultiDiGraph, clique_graph: nx.Graph, node: str, attributes: Dict) -> List:
    """
    Get category for a node based on its equivalent nodes in a graph.

    Parameters
    ----------
    target_graph: networkx.MultiDiGraph
        The original graph
    clique_graph: networkx.Graph
        The clique graph
    node: str
        Node identifier
    attributes: Dict
        Node's attributes

    Returns
    -------
    List
        Category for the node

    """
    category: List = []
    for u, v, data in clique_graph.edges(node, data=True):
        if data['edge_label'] == SAME_AS:
            if u == node:
                category = clique_graph.nodes[v]['category']
                break
            elif v == node:
                category = clique_graph.nodes[u]['category']
                break
            update = {node: {'category': category}}
            nx.set_node_attributes(clique_graph, update)
    return category


def validate_clique_category(target_graph: nx.MultiDiGraph, clique_graph: nx.Graph, clique: List) -> Tuple[Optional[str], List[Any]]:
    """
    For nodes in a clique, validate the category for each node to make sure that
    all nodes in a clique are of the same type.

    Parameters
    ----------
    target_graph: networkx.MultiDiGraph
        The original graph
    clique_graph: networkx.Graph
        The clique graph
    clique: List
        A list of nodes from a clique

    Returns
    -------
    Tuple[Optional[str], List[Any]]
        A tuple of clique category string and a list of invalid nodes

    """
    invalid_nodes: List = []
    all_categories: List = []
    clique_category: Optional[str] = None
    for node in clique:
        node_data = clique_graph.nodes[node]
        if 'category' in node_data and len(node_data['category']) > 0:
            all_categories.append(node_data['category'][0])

    if len(all_categories) > 0:
        (clique_category, clique_category_ancestors) = get_the_most_specific_category(all_categories)
        for node in clique:
            data = clique_graph.nodes[node]
            node_category = data['category'][0]
            log.debug(f"node_category: {node_category}")
            if node_category not in clique_category_ancestors:
                invalid_nodes.append(node)
                log.debug(f"clique category '{clique_category}' does not match node: {data}")
            # TODO: check if node category is a subclass of any of the ancestors via other ontologies
    return clique_category, invalid_nodes


def get_the_most_specific_category(categories: List) -> Tuple[Optional[Any], List[Any]]:
    """
    From a list of categories, get ancestors for all.
    The category with the longest ancestor is considered to be the most specific.

    .. note::
        This assumes that all the category in ``categories`` are part of the same closure.

    Parameters
    ----------
    categories: List
        A list of categories

    Returns
    -------
    Tuple[Optional[Any], List[Any]]
        A tuple of the most specific category and a list of ancestors of that category

    """
    most_specific_category: Optional[str] = None
    most_specific_category_ancestors: List = []
    for category in categories:
        log.debug("category: {}".format(category))
        element = get_biolink_element(category)
        if element:
            # category exists in Biolink Model as a class or as an alias to a class
            mapped_category = element['name']
            ancestors = get_biolink_ancestors(mapped_category)
            log.debug(f"ancestors: {ancestors}")
            if len(ancestors) > len(most_specific_category_ancestors):
                # the category with the longest list of ancestors will be the most specific category
                most_specific_category = category
                most_specific_category_ancestors = ancestors
    return most_specific_category, most_specific_category_ancestors


def get_leader_by_annotation(target_graph: nx.MultiDiGraph, clique_graph: nx.Graph, clique: List, leader_annotation: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Get leader by searching for leader annotation property in any of the nodes in a given clique.

    Parameters
    ----------
    target_graph: networkx.MultiDiGraph
        The original graph
    clique_graph: networkx.Graph
        The clique graph
    clique: List
        A list of nodes from a clique
    leader_annotation: str
        The field on a node that signifies that the node is the leader of a clique

    Returns
    -------
    Tuple[Optional[str], Optional[str]]
        A tuple containing the node that has been elected as the leader and the election strategy

    """
    leader = None
    election_strategy = None
    for node in clique:
        attributes = clique_graph.nodes[node]
        if leader_annotation in attributes:
            if isinstance(attributes[leader_annotation], str):
                v = attributes[leader_annotation]
                if v == "true" or v == "True":
                    leader = node
            elif isinstance(attributes[leader_annotation], list):
                v = attributes[leader_annotation][0]
                if isinstance(v, str):
                    if v == "true" or v == "True":
                        leader = node
                elif isinstance(v, bool):
                    if eval(str(v)):
                        leader = node
            elif isinstance(attributes[leader_annotation], bool):
                v = attributes[leader_annotation]
                if eval(str(v)):
                    leader = node
    if leader:
        election_strategy = 'LEADER_ANNOTATION'
        log.debug(f"Elected leader '{leader}' via LEADER_ANNOTATION")

    return leader, election_strategy


def get_leader_by_prefix_priority(target_graph: nx.MultiDiGraph, clique_graph: nx.Graph, clique: List, prefix_priority_list: List) -> Tuple[Optional[str], Optional[str]]:
    """
    Get leader from clique based on a given prefix priority.

    Parameters
    ----------
    target_graph: networkx.MultiDiGraph
        The original graph
    clique_graph: networkx.Graph
        The clique graph
    clique: List
        A list of nodes that correspond to a clique
    prefix_priority_list: List
        A list of prefixes in descending priority

    Returns
    -------
    Tuple[Optional[str], Optional[str]]
        A tuple containing the node that has been elected as the leader and the election strategy

    """
    leader = None
    election_strategy = None
    for prefix in prefix_priority_list:
        log.debug(f"Checking for prefix {prefix} in {clique}")
        leader = next((x for x in clique if prefix in x), None)
        if leader:
            election_strategy = "PREFIX_PRIORITIZATION"
            log.debug(f"Elected leader '{leader}' via {election_strategy}")
            break
    return leader, election_strategy


def get_leader_by_sort(target_graph: nx.MultiDiGraph, clique_graph: nx.Graph, clique: List) -> Tuple[Optional[str], Optional[str]]:
    """
    Get leader from clique based on the first selection from an alphabetical sort of the node id prefixes.

    Parameters
    ----------
    target_graph: networkx.MultiDiGraph
        The original graph
    clique_graph: networkx.Graph
        The clique graph
    clique: List
        A list of nodes that correspond to a clique

    Returns
    -------
    Tuple[Optional[str], Optional[str]]
        A tuple containing the node that has been elected as the leader and the election strategy

    """
    election_strategy = 'ALPHABETICAL_SORT'
    prefixes = [x.split(':', 1)[0] for x in clique]
    prefixes.sort()
    leader_prefix = prefixes[0]
    log.debug(f"clique: {clique} leader_prefix: {leader_prefix}")
    leader = [x for x in clique if leader_prefix in x]
    if leader:
        log.debug(f"Elected leader '{leader}' via {election_strategy}")
    return leader[0], election_strategy
