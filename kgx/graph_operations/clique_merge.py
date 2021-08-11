import copy
from typing import Tuple, Optional, Dict, List, Any, Set, Union

import networkx as nx
from ordered_set import OrderedSet

from kgx.config import get_logger
from kgx.graph.base_graph import BaseGraph
from kgx.utils.kgx_utils import (
    get_prefix_prioritization_map,
    get_biolink_element,
    get_biolink_ancestors,
    current_time_in_millis,
    format_biolink_category,
    generate_edge_key,
    get_toolkit,
)

log = get_logger()
toolkit = get_toolkit()
SAME_AS = "biolink:same_as"
SUBCLASS_OF = "biolink:subclass_of"
LEADER_ANNOTATION = "clique_leader"
ORIGINAL_SUBJECT_PROPERTY = "_original_subject"
ORIGINAL_OBJECT_PROPERTY = "_original_object"


def clique_merge(
    target_graph: BaseGraph,
    leader_annotation: str = None,
    prefix_prioritization_map: Optional[Dict[str, List[str]]] = None,
    category_mapping: Optional[Dict[str, str]] = None,
    strict: bool = True,
) -> Tuple[BaseGraph, nx.MultiDiGraph]:
    """

    Parameters
    ----------
    target_graph: kgx.graph.base_graph.BaseGraph
        The original graph
    leader_annotation: str
        The field on a node that signifies that the node is the leader of a clique
    prefix_prioritization_map: Optional[Dict[str, List[str]]]
        A map that gives a prefix priority for one or more categories
    category_mapping: Optional[Dict[str, str]]
        Mapping for non-Biolink Model categories to Biolink Model categories
    strict: bool
        Whether or not to merge nodes in a clique that have conflicting node categories

    Returns
    -------
    Tuple[kgx.graph.base_graph.BaseGraph, networkx.MultiDiGraph]
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
    elect_leader(
        target_graph,
        clique_graph,
        leader_annotation,
        prefix_prioritization_map,
        category_mapping,
        strict,
    )
    end = current_time_in_millis()
    log.info(f"Total time taken to elect leaders for all cliques: {end - start} ms")

    start = current_time_in_millis()
    graph = consolidate_edges(target_graph, clique_graph, leader_annotation)
    end = current_time_in_millis()
    log.info(f"Total time taken to consolidate edges in target graph: {end - start} ms")
    return graph, clique_graph


def build_cliques(target_graph: BaseGraph) -> nx.MultiDiGraph:
    """
    Builds a clique graph from ``same_as`` edges in ``target_graph``.

    Parameters
    ----------
    target_graph: kgx.graph.base_graph.BaseGraph
        An instance of BaseGraph that contains nodes and edges

    Returns
    -------
    networkx.MultiDiGraph
        The clique graph with only ``same_as`` edges

    """
    clique_graph = nx.MultiDiGraph()
    for n, data in target_graph.nodes(data=True):
        if "same_as" in data:
            new_data = copy.deepcopy(data)
            del new_data["same_as"]
            clique_graph.add_node(n, **new_data)
            for s in data["same_as"]:
                edge_data1 = {"subject": n, "predicate": SAME_AS, "object": s}
                if "provided_by" in data:
                    edge_data1["provided_by"] = data["provided_by"]
                clique_graph.add_edge(n, s, **edge_data1)
                edge_data2 = {"subject": s, "predicate": SAME_AS, "object": n}
                if "provided_by" in data:
                    edge_data2["provided_by"] = data["provided_by"]
                clique_graph.add_edge(s, n, **edge_data2)
    for u, v, data in target_graph.edges(data=True):
        if "predicate" in data and data["predicate"] == SAME_AS:
            # load all biolink:same_as edges to clique_graph
            clique_graph.add_node(u, **target_graph.nodes()[u])
            clique_graph.add_node(v, **target_graph.nodes()[v])
            clique_graph.add_edge(u, v, **data)
            clique_graph.add_edge(
                v,
                u,
                **{
                    "subject": v,
                    "predicate": data["predicate"],
                    "object": v,
                    "relation": data["relation"],
                },
            )
    return clique_graph


def elect_leader(
    target_graph: BaseGraph,
    clique_graph: nx.MultiDiGraph,
    leader_annotation: str,
    prefix_prioritization_map: Optional[Dict[str, List[str]]],
    category_mapping: Optional[Dict[str, str]],
    strict: bool = True,
) -> BaseGraph:
    """
    Elect leader for each clique in a graph.

    Parameters
    ----------
    target_graph: kgx.graph.base_graph.BaseGraph
        The original graph
    clique_graph: networkx.Graph
        The clique graph
    leader_annotation: str
        The field on a node that signifies that the node is the leader of a clique
    prefix_prioritization_map: Optional[Dict[str, List[str]]]
        A map that gives a prefix priority for one or more categories
    category_mapping: Optional[Dict[str, str]]
        Mapping for non-Biolink Model categories to Biolink Model categories
    strict: bool
        Whether or not to merge nodes in a clique that have conflicting node categories

    Returns
    -------
    kgx.graph.base_graph.BaseGraph
        The updated target graph

    """
    cliques = list(nx.strongly_connected_components(clique_graph))
    log.info(f"Total cliques in clique graph: {len(cliques)}")
    count = 0
    update_dict = {}
    for clique in cliques:
        log.info(
            f"Processing clique: {clique} with {[clique_graph.nodes()[x]['category'] if 'category' in clique_graph.nodes()[x] else None for x in clique]}"
        )
        update_node_categories(
            target_graph, clique_graph, clique, category_mapping, strict
        )
        clique_category, clique_category_ancestors = get_clique_category(
            clique_graph, clique
        )
        log.debug(f"Clique category: {clique_category}")
        invalid_nodes = set()
        for n in clique:
            data = clique_graph.nodes()[n]
            if "_excluded_from_clique" in data and data["_excluded_from_clique"]:
                log.info(
                    f"Removing invalid node {n} from clique graph; node marked to be excluded"
                )
                clique_graph.remove_node(n)
                invalid_nodes.add(n)
            if data["category"][0] not in clique_category_ancestors:
                log.info(
                    f"Removing invalid node {n} from the clique graph; node category {data['category'][0]} not in CCA: {clique_category_ancestors}"
                )
                clique_graph.remove_node(n)
                invalid_nodes.add(n)

        filtered_clique = [x for x in clique if x not in invalid_nodes]
        if filtered_clique:
            if clique_category:
                # First check for LEADER_ANNOTATION property
                leader, election_strategy = get_leader_by_annotation(
                    target_graph, clique_graph, filtered_clique, leader_annotation
                )
                if not leader:
                    # Leader is None; use prefix prioritization strategy
                    log.debug(
                        "Could not elect clique leader by looking for LEADER_ANNOTATION property; "
                        "Using prefix prioritization instead"
                    )
                    if (
                        prefix_prioritization_map
                        and clique_category in prefix_prioritization_map.keys()
                    ):
                        leader, election_strategy = get_leader_by_prefix_priority(
                            target_graph,
                            clique_graph,
                            filtered_clique,
                            prefix_prioritization_map[clique_category],
                        )
                    else:
                        log.debug(
                            f"No prefix order found for category '{clique_category}' in PREFIX_PRIORITIZATION_MAP"
                        )

                if not leader:
                    # Leader is None; fall back to alphabetical sort on prefixes
                    log.debug(
                        "Could not elect clique leader by PREFIX_PRIORITIZATION; Using alphabetical sort on prefixes"
                    )
                    leader, election_strategy = get_leader_by_sort(
                        target_graph, clique_graph, filtered_clique
                    )

                log.debug(
                    f"Elected {leader} as leader via {election_strategy} for clique {filtered_clique}"
                )
                update_dict[leader] = {
                    LEADER_ANNOTATION: True,
                    "election_strategy": election_strategy,
                }
                count += 1

    nx.set_node_attributes(clique_graph, update_dict)
    target_graph.set_node_attributes(target_graph, update_dict)
    log.info(f"Total merged cliques: {count}")
    return target_graph


def consolidate_edges(
    target_graph: BaseGraph, clique_graph: nx.MultiDiGraph, leader_annotation: str
) -> BaseGraph:
    """
    Move all edges from nodes in a clique to the clique leader.

    Original subject and object of a node are preserved via ``ORIGINAL_SUBJECT_PROPERTY`` and ``ORIGINAL_OBJECT_PROPERTY``

    Parameters
    ----------
    target_graph: kgx.graph.base_graph.BaseGraph
        The original graph
    clique_graph: networkx.MultiDiGraph
        The clique graph
    leader_annotation: str
        The field on a node that signifies that the node is the leader of a clique

    Returns
    -------
    kgx.graph.base_graph.BaseGraph
        The target graph where all edges from nodes in a clique are moved to clique leader

    """
    cliques = list(nx.strongly_connected_components(clique_graph))
    log.info(f"Consolidating edges in {len(cliques)} cliques")
    for clique in cliques:
        log.debug(f"Processing clique: {clique}")
        leaders: List = [
            x
            for x in clique
            if leader_annotation in clique_graph.nodes()[x]
            and clique_graph.nodes()[x][leader_annotation]
        ]
        if len(leaders) == 0:
            log.debug("No leader elected for clique {}; skipping".format(clique))
            continue
        leader: str = leaders[0]
        # update nodes in target graph
        target_graph.set_node_attributes(
            target_graph,
            {
                leader: {
                    leader_annotation: clique_graph.nodes()[leader].get(
                        leader_annotation
                    ),
                    "election_strategy": clique_graph.nodes()[leader].get(
                        "election_strategy"
                    ),
                }
            },
        )
        leader_equivalent_identifiers = set([x for x in clique_graph.neighbors(leader)])
        for node in clique:
            if node == leader:
                continue
            log.debug(f"Looking for in_edges for {node}")
            in_edges = target_graph.in_edges(node, keys=False, data=True)
            filtered_in_edges = [x for x in in_edges if x[2]["predicate"] != SAME_AS]
            equiv_in_edges = [x for x in in_edges if x[2]["predicate"] == SAME_AS]
            log.debug(f"Moving {len(in_edges)} in-edges from {node} to {leader}")
            for u, v, edge_data in filtered_in_edges:
                key = generate_edge_key(u, edge_data["predicate"], v)
                target_graph.remove_edge(u, v, edge_key=key)
                edge_data[ORIGINAL_SUBJECT_PROPERTY] = edge_data["subject"]
                edge_data[ORIGINAL_OBJECT_PROPERTY] = edge_data["object"]
                edge_data["object"] = leader
                key = generate_edge_key(u, edge_data["predicate"], leader)
                if (
                    edge_data["subject"] == edge_data["object"]
                    and edge_data["predicate"] == SUBCLASS_OF
                ):
                    continue
                target_graph.add_edge(
                    edge_data["subject"], edge_data["object"], key, **edge_data
                )

            log.debug(f"Looking for out_edges for {node}")
            out_edges = target_graph.out_edges(node, keys=False, data=True)
            filtered_out_edges = [x for x in out_edges if x[2]["predicate"] != SAME_AS]
            equiv_out_edges = [x for x in out_edges if x[2]["predicate"] == SAME_AS]
            log.debug(f"Moving {len(out_edges)} out-edges from {node} to {leader}")
            for u, v, edge_data in filtered_out_edges:
                key = generate_edge_key(u, edge_data["predicate"], v)
                target_graph.remove_edge(u, v, edge_key=key)
                edge_data[ORIGINAL_SUBJECT_PROPERTY] = edge_data["subject"]
                edge_data[ORIGINAL_OBJECT_PROPERTY] = edge_data["object"]
                edge_data["subject"] = leader
                key = generate_edge_key(leader, edge_data["predicate"], v)
                if (
                    edge_data["subject"] == edge_data["object"]
                    and edge_data["predicate"] == SUBCLASS_OF
                ):
                    continue
                target_graph.add_edge(
                    edge_data["subject"], edge_data["object"], key, **edge_data
                )

            log.debug(f"equiv out edges: {equiv_out_edges}")
            equivalent_identifiers = set()
            for u, v, edge_data in equiv_in_edges:
                if u != leader:
                    equivalent_identifiers.add(u)
                if v != leader:
                    equivalent_identifiers.add(v)
                target_graph.remove_edge(
                    u, v, edge_key=generate_edge_key(u, SAME_AS, v)
                )

            log.debug(f"equiv out edges: {equiv_out_edges}")
            for u, v, edge_data in equiv_out_edges:
                if u != leader:
                    log.debug(f"{u} is an equivalent identifier of leader {leader}")
                    equivalent_identifiers.add(u)
                if v != leader:
                    log.debug(f"{v} is an equivalent identifier of leader {leader}")
                    equivalent_identifiers.add(v)
                target_graph.remove_edge(
                    u, v, edge_key=generate_edge_key(u, SAME_AS, v)
                )

            leader_equivalent_identifiers.update(equivalent_identifiers)

        log.debug(
            f"setting same_as property to leader node with {leader_equivalent_identifiers}"
        )
        target_graph.set_node_attributes(
            target_graph, {leader: {"same_as": list(leader_equivalent_identifiers)}}
        )
        log.debug(
            f"removing equivalent nodes of leader: {leader_equivalent_identifiers}"
        )
        for n in leader_equivalent_identifiers:
            target_graph.remove_node(n)
    return target_graph


def update_node_categories(
    target_graph: BaseGraph,
    clique_graph: nx.MultiDiGraph,
    clique: List,
    category_mapping: Optional[Dict[str, str]],
    strict: bool = True,
) -> List:
    """
    For a given clique, get category for each node in clique and validate against Biolink Model,
    mapping to Biolink Model category where needed.

    For example, If a node has ``biolink:Gene`` as its category, then this method adds all of its ancestors.

    Parameters
    ----------
    target_graph: kgx.graph.base_graph.BaseGraph
        The original graph
    clique_graph: networkx.Graph
        The clique graph
    clique: List
        A list of nodes from a clique
    category_mapping: Optional[Dict[str, str]]
        Mapping for non-Biolink Model categories to Biolink Model categories
    strict: bool
        Whether or not to merge nodes in a clique that have conflicting node categories

    Returns
    -------
    List
        The clique

    """
    updated_clique_graph_properties = {}
    updated_target_graph_properties = {}
    for node in clique:
        # For each node in a clique, get its category property
        data = clique_graph.nodes()[node]
        if "category" in data:
            categories = data["category"]
        else:
            categories = get_category_from_equivalence(
                target_graph, clique_graph, node, data
            )

        # differentiate between valid and invalid categories
        (
            valid_biolink_categories,
            invalid_biolink_categories,
            invalid_categories,
        ) = check_all_categories(categories)
        log.debug(
            f"valid biolink categories: {valid_biolink_categories} invalid biolink categories: {invalid_biolink_categories} invalid_categories: {invalid_categories}"
        )
        # extend categories to have the longest list of ancestors
        extended_categories: List = []
        for x in valid_biolink_categories:
            ancestors = get_biolink_ancestors(x)
            if len(ancestors) > len(extended_categories):
                extended_categories.extend(ancestors)
        log.debug(f"Extended categories: {extended_categories}")
        clique_graph_update_dict: Dict = {"category": list(extended_categories)}
        target_graph_update_dict: Dict = {}

        if invalid_biolink_categories:
            if strict:
                clique_graph_update_dict["_excluded_from_clique"] = True
                target_graph_update_dict["_excluded_from_clique"] = True
            clique_graph_update_dict[
                "invalid_biolink_category"
            ] = invalid_biolink_categories
            target_graph_update_dict[
                "invalid_biolink_category"
            ] = invalid_biolink_categories

        if invalid_categories:
            clique_graph_update_dict["_invalid_category"] = invalid_categories
            target_graph_update_dict["_invalid_category"] = invalid_categories

        updated_clique_graph_properties[node] = clique_graph_update_dict
        updated_target_graph_properties[node] = target_graph_update_dict

    nx.set_node_attributes(clique_graph, updated_clique_graph_properties)
    target_graph.set_node_attributes(target_graph, updated_target_graph_properties)
    return clique


def get_clique_category(
    clique_graph: nx.MultiDiGraph, clique: List
) -> Tuple[str, List]:
    """
    Given a clique, identify the category of the clique.

    Parameters
    ----------
    clique_graph: nx.MultiDiGraph
        Clique graph
    clique: List
        A list of nodes in clique

    Returns
    -------
    Tuple[str, list]
        A tuple of clique category and its ancestors

    """
    l = [clique_graph.nodes()[x]["category"] for x in clique]
    u = OrderedSet.union(*l)
    uo = sort_categories(u)
    log.debug(f"outcome of union (sorted): {uo}")
    clique_category = uo[0]
    clique_category_ancestors = get_biolink_ancestors(uo[0])
    return clique_category, clique_category_ancestors


def check_categories(
    categories: List, closure: List, category_mapping: Optional[Dict[str, str]] = None
) -> Tuple[List, List, List]:
    """
    Check categories to ensure whether values in ``categories`` are valid biolink categories.
    Valid biolink categories are classes that descend from 'NamedThing'.
    Mixins, while valid ancestors, are not valid categories.

    Parameters
    ----------
    categories: List
        A list of categories to check
    closure: List
        A list of nodes in a clique
    category_mapping: Optional[Dict[str, str]]
        A map that provides mapping from a non-biolink category to a biolink category

    Returns
    -------
    Tuple[List, List, List]
        A tuple consisting of valid biolink categories, invalid biolink categories, and invalid categories

    """
    valid_biolink_categories = []
    invalid_biolink_categories = []
    invalid_categories = []
    tk = get_toolkit()
    for x in categories:
        # use the toolkit to check if the declared category is actually a mixin.
        if tk.is_mixin(x):
            invalid_categories.append(x)
            continue
        # get biolink element corresponding to category
        element = get_biolink_element(x)
        if element:
            mapped_category = format_biolink_category(element["name"])
            if mapped_category in closure:
                valid_biolink_categories.append(x)
            else:
                log.warning(f"category '{mapped_category}' not in closure: {closure}")
                if category_mapping:
                    mapped = category_mapping[x] if x in category_mapping.keys() else x
                    if mapped not in closure:
                        log.warning(
                            f"category '{mapped_category}' is not in category_mapping."
                        )
                        invalid_biolink_categories.append(x)
                else:
                    invalid_biolink_categories.append(x)
        else:
            log.warning(f"category '{x}' is not in Biolink Model")
            invalid_categories.append(x)
            continue
    return valid_biolink_categories, invalid_biolink_categories, invalid_categories


def check_all_categories(categories) -> Tuple[List, List, List]:
    """
    Check all categories in ``categories``.

    Parameters
    ----------
    categories: List
        A list of categories

    Returns
    -------
    Tuple[List, List, List]
        A tuple consisting of valid biolink categories, invalid biolink categories, and invalid categories

    Note: the sort_categories method will re-arrange the passed in category list according to the distance
    of each list member from the top of their hierarchy.  Each category's hierarchy is made up of its
    'is_a' and mixin ancestors.

    """
    previous: List = []
    valid_biolink_categories: List = []
    invalid_biolink_categories: List = []
    invalid_categories: List = []
    sc: List = sort_categories(categories)
    for c in sc:
        if previous:
            vbc, ibc, ic = check_categories(
                [c], get_biolink_ancestors(previous[0]), None
            )
        else:
            vbc, ibc, ic = check_categories([c], get_biolink_ancestors(c), None)
        if vbc:
            valid_biolink_categories.extend(vbc)
        if ic:
            invalid_categories.extend(ic)
        if ibc:
            invalid_biolink_categories.extend(ibc)
        else:
            previous = vbc
    return valid_biolink_categories, invalid_biolink_categories, invalid_categories


def sort_categories(categories: Union[List, Set, OrderedSet]) -> List:
    """
    Sort a list of categories from most specific to the most generic.

    Parameters
    ----------
    categories: Union[List, Set, OrderedSet]
        A list of categories

    Returns
    -------
    List
        A sorted list of categories where sorted means that the first element in the list returned
        has the most number of parents in the class hierarchy.

    """
    weighted_categories = []
    for c in categories:
        weighted_categories.append((len(get_biolink_ancestors(c)), c))
    sorted_categories = sorted(weighted_categories, key=lambda x: x[0], reverse=True)
    return [x[1] for x in sorted_categories]


def get_category_from_equivalence(
    target_graph: BaseGraph, clique_graph: nx.MultiDiGraph, node: str, attributes: Dict
) -> List:
    """
    Get category for a node based on its equivalent nodes in a graph.

    Parameters
    ----------
    target_graph: kgx.graph.base_graph.BaseGraph
        The original graph
    clique_graph: networkx.MultiDiGraph
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
        if data["predicate"] == SAME_AS:
            if u == node:
                if "category" in clique_graph.nodes()[v]:
                    category = clique_graph.nodes()[v]["category"]
                    break
            elif v == node:
                if "category" in clique_graph.nodes()[u]:
                    category = clique_graph.nodes()[u]["category"]
                    break
            update = {node: {"category": category}}
            nx.set_node_attributes(clique_graph, update)
    return category


def get_leader_by_annotation(
    target_graph: BaseGraph,
    clique_graph: nx.MultiDiGraph,
    clique: List,
    leader_annotation: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Get leader by searching for leader annotation property in any of the nodes in a given clique.

    Parameters
    ----------
    target_graph: kgx.graph.base_graph.BaseGraph
        The original graph
    clique_graph: networkx.MultiDiGraph
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
        attributes = clique_graph.nodes()[node]
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
        election_strategy = "LEADER_ANNOTATION"
        log.debug(f"Elected leader '{leader}' via LEADER_ANNOTATION")

    return leader, election_strategy


def get_leader_by_prefix_priority(
    target_graph: BaseGraph,
    clique_graph: nx.MultiDiGraph,
    clique: List,
    prefix_priority_list: List,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Get leader from clique based on a given prefix priority.

    Parameters
    ----------
    target_graph: kgx.graph.base_graph.BaseGraph
        The original graph
    clique_graph: networkx.MultiDiGraph
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


def get_leader_by_sort(
    target_graph: BaseGraph, clique_graph: nx.MultiDiGraph, clique: List
) -> Tuple[Optional[str], Optional[str]]:
    """
    Get leader from clique based on the first selection from an alphabetical sort of the node id prefixes.

    Parameters
    ----------
    target_graph: kgx.graph.base_graph.BaseGraph
        The original graph
    clique_graph: networkx.MultiDiGraph
        The clique graph
    clique: List
        A list of nodes that correspond to a clique

    Returns
    -------
    Tuple[Optional[str], Optional[str]]
        A tuple containing the node that has been elected as the leader and the election strategy

    """
    election_strategy = "ALPHABETICAL_SORT"
    prefixes = [x.split(":", 1)[0] for x in clique]
    prefixes.sort()
    leader_prefix = prefixes[0]
    log.debug(f"clique: {clique} leader_prefix: {leader_prefix}")
    leader = [x for x in clique if leader_prefix in x]
    if leader:
        log.debug(f"Elected leader '{leader}' via {election_strategy}")
    return leader[0], election_strategy
