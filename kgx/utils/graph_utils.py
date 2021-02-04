from typing import List, Set, Dict, Optional
import stringcase
from cachetools import cached

from kgx.config import get_logger
from kgx.graph.base_graph import BaseGraph
from kgx.utils.kgx_utils import get_toolkit, get_cache, get_curie_lookup_service, generate_edge_key, \
    CORE_NODE_PROPERTIES, CORE_EDGE_PROPERTIES, current_time_in_millis
from kgx.prefix_manager import PrefixManager

ONTOLOGY_PREFIX_MAP: Dict = {}
ONTOLOGY_GRAPH_CACHE: Dict = {}

log = get_logger()


def get_parents(graph: BaseGraph, node: str, relations: List[str] = None) -> List[str]:
    """
    Return all direct `parents` of a specified node, filtered by ``relations``.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        Graph to traverse
    node: str
        node identifier
    relations: List[str]
       list of relations

    Returns
    -------
    List[str]
        A list of parent node(s)

    """
    parents = []
    if graph.has_node(node):
        out_edges = [x for x in graph.out_edges(node, keys=False, data=True)]
        if relations is None:
            parents = [x[1] for x in out_edges]
        else:
            parents = [x[1] for x in out_edges if x[2]['predicate'] in relations]
    return parents


def get_ancestors(graph: BaseGraph, node: str, relations: List[str] = None) -> List[str]:
    """
    Return all `ancestors` of specified node, filtered by ``relations``.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        Graph to traverse
    node: str
        node identifier
    relations: List[str]
       list of relations

    Returns
    -------
    List[str]
        A list of ancestor nodes

    """
    seen = []
    nextnodes = [node]
    while len(nextnodes) > 0:
        nn = nextnodes.pop()
        if nn not in seen:
            seen.append(nn)
            nextnodes += get_parents(graph, nn, relations=relations)
    seen.remove(node)
    return seen

@cached(get_cache())
def get_category_via_superclass(graph: BaseGraph, curie: str, load_ontology: bool = True) -> Set[str]:
    """
    Get category for a given CURIE by tracing its superclass, via ``subclass_of`` hierarchy,
    and getting the most appropriate category based on the superclass.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        Graph to traverse
    curie: str
        Input CURIE
    load_ontology: bool
        Determines whether to load ontology, based on CURIE prefix, or to simply
        rely on ``subclass_of`` hierarchy from graph

    Returns
    -------
    Set[str]
        A set containing one (or more) category for the given CURIE

    """
    log.debug("curie: {}".format(curie))
    new_categories = []
    toolkit = get_toolkit()
    if PrefixManager.is_curie(curie):
        ancestors = get_ancestors(graph, curie, relations=['subclass_of'])
        if len(ancestors) == 0 and load_ontology:
            cls = get_curie_lookup_service()
            ontology_graph = cls.ontology_graph
            new_categories += [x for x in get_category_via_superclass(ontology_graph, curie, False)]
        log.debug("Ancestors for CURIE {} via subClassOf: {}".format(curie, ancestors))
        seen = []
        for anc in ancestors:
            mapping = toolkit.get_by_mapping(anc)
            seen.append(anc)
            if mapping:
                # there is direct mapping to BioLink Model
                log.debug("Ancestor {} mapped to {}".format(anc, mapping))
                seen_labels = [graph.nodes()[x]['name'] for x in seen if 'name' in graph.nodes()[x]]
                new_categories += [x for x in seen_labels]
                new_categories += [x for x in toolkit.ancestors(mapping)]
                break
    return set(new_categories)


def curie_lookup(curie: str) -> Optional[str]:
    """
    Given a CURIE, find its label.

    This method first does a lookup in predefined maps. If none found,
    it makes use of CurieLookupService to look for the CURIE in a set
    of preloaded ontologies.

    Parameters
    ----------
    curie: str
        A CURIE

    Returns
    -------
    Optional[str]
        The label corresponding to the given CURIE

    """
    cls = get_curie_lookup_service()
    name: Optional[str] = None
    prefix = PrefixManager.get_prefix(curie)
    if prefix in ['OIO', 'OWL', 'owl', 'OBO', 'rdfs']:
        name = stringcase.snakecase(curie.split(':', 1)[1])
    elif curie in cls.curie_map:
        name = cls.curie_map[curie]
    elif curie in cls.ontology_graph:
        name = cls.ontology_graph.nodes()[curie]['name']
    return name


def remap_node_identifier(graph: BaseGraph, category: str, alternative_property: str, prefix=None) -> BaseGraph:
    """
    Remap a node's 'id' attribute with value from a node's ``alternative_property`` attribute.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    category: string
        category referring to nodes whose 'id' needs to be remapped
    alternative_property: string
        property name from which the new value is pulled from
    prefix: string
        signifies that the value for ``alternative_property`` is a list
        and the ``prefix`` indicates which value to pick from the list

    Returns
    -------
    kgx.graph.base_graph.BaseGraph
        The modified graph

    """
    mapping: Dict = {}
    for nid, data in graph.nodes(data=True):
        node_data = data.copy()
        if 'category' in node_data and category not in node_data['category']:
            continue

        if alternative_property in node_data:
            alternative_values = node_data[alternative_property]
            if isinstance(alternative_values, (list, set, tuple)):
                if prefix:
                    for v in alternative_values:
                        if prefix in v:
                            # take the first occurring value that contains the given prefix
                            mapping[nid] = {'id': v}
                            break
                else:
                    # no prefix defined; pick the 1st one from list
                    mapping[nid] = {'id': next(iter(alternative_values))}
            elif isinstance(alternative_values, str):
                if prefix:
                    if alternative_values.startswith(prefix):
                        mapping[nid] = {'id': alternative_values}
                else:
                    # no prefix defined
                    mapping[nid] = {'id': alternative_values}
            else:
                log.error(f"Cannot use {alternative_values} from alternative_property {alternative_property}")

    graph.set_node_attributes(graph, attributes=mapping)
    graph.relabel_nodes(graph, {k: list(v.values())[0] for k, v in mapping.items()})

    # update 'subject' of all outgoing edges
    update_edge_keys = {}
    updated_subject_values = {}
    updated_object_values = {}
    for u, v, k, edge_data in graph.edges(data=True, keys=True):
        if u is not edge_data['subject']:
            updated_subject_values[(u, v, k)] = {'subject': u}
            update_edge_keys[(u, v, k)] = {'edge_key': generate_edge_key(u, edge_data['predicate'], v)}
        if v is not edge_data['object']:
            updated_object_values[(u, v, k)] = {'object': v}
            update_edge_keys[(u, v, k)] = {'edge_key': generate_edge_key(u, edge_data['predicate'], v)}

    graph.set_edge_attributes(graph, attributes=updated_subject_values)
    graph.set_edge_attributes(graph, attributes=updated_object_values)
    graph.set_edge_attributes(graph, attributes=update_edge_keys)

    return graph


def remap_node_property(graph: BaseGraph, category: str, old_property: str, new_property: str) -> None:
    """
    Remap the value in node ``old_property`` attribute with value
    from node ``new_property`` attribute.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    category: string
        Category referring to nodes whose property needs to be remapped
    old_property: string
        old property name whose value needs to be replaced
    new_property: string
        new property name from which the value is pulled from

    """
    mapping = {}
    if old_property in CORE_NODE_PROPERTIES:
        raise AttributeError(f"node property {old_property} cannot be modified as it is a core property.")

    for nid, data in graph.nodes(data=True):
        node_data = data.copy()
        if category in node_data and category not in node_data['category']:
            continue
        if new_property in node_data:
            mapping[nid] = {old_property: node_data[new_property]}
    graph.set_node_attributes(graph, attributes=mapping)


def remap_edge_property(graph: BaseGraph, edge_predicate: str, old_property: str, new_property: str) -> None:
    """
    Remap the value in an edge ``old_property`` attribute with value
    from edge ``new_property`` attribute.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    edge_predicate: string
        edge_predicate referring to edges whose property needs to be remapped
    old_property: string
        Old property name whose value needs to be replaced
    new_property: string
        New property name from which the value is pulled from

    """
    mapping = {}
    if old_property in CORE_EDGE_PROPERTIES:
        raise AttributeError(f"edge property {old_property} cannot be modified as it is a core property.")
    for u, v, k, data in graph.edges(data=True, keys=True):
        edge_data = data.copy()
        if edge_predicate is not edge_data['predicate']:
            continue
        if new_property in edge_data:
            mapping[(u, v, k)] = {old_property: edge_data[new_property]}
    graph.set_edge_attributes(graph, attributes=mapping)


def fold_predicate(graph: BaseGraph, predicate: str, remove_prefix: bool = False) -> None:
    """
    Fold predicate as node property where every edge with ``predicate``
    will be folded as a node property.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    predicate: str
        The predicate to fold
    remove_prefix: bool
        Whether or not to remove prefix from the predicate (``False``, by default)

    """
    node_cache = []
    edge_cache = []
    start = current_time_in_millis()
    p = predicate.split(':', 1)[1] if remove_prefix else predicate
    for u, v, k, data in graph.edges(keys=True, data=True):
        if data['predicate'] == predicate:
            node_cache.append((u, p, v))
            edge_cache.append((u, v, k))
    while node_cache:
        n = node_cache.pop()
        graph.add_node_attribute(*n)
    while edge_cache:
        e = edge_cache.pop()
        graph.remove_edge(*e)
    end = current_time_in_millis()
    log.info(f"Time taken: {end - start} ms")


def unfold_node_property(graph: BaseGraph, node_property: str, prefix: Optional[str] = None) -> None:
    """
    Unfold node property as a predicate where every node with ``node_property``
    will be unfolded as an edge.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    node_property: str
        The node property to unfold
    prefix: Optional[str]
        The prefix to use

    """
    node_cache = []
    edge_cache = []
    start = current_time_in_millis()
    p = f"{prefix}:{node_property}" if prefix else node_property
    for n, data in graph.nodes(data=True):
        sub = n
        if node_property in data:
            obj = data[node_property]
            edge_cache.append((sub, obj, p))
            node_cache.append((n, node_property))
    while edge_cache:
        e = edge_cache.pop()
        graph.add_edge(*e, **{'subject': e[0], 'object': e[1], 'predicate': e[2], 'relation': e[2]})
    while node_cache:
        n = node_cache.pop()
        del graph.nodes()[n[0]][n[1]]
    end = current_time_in_millis()
    log.info(f"Time taken: {end - start} ms")


def remove_singleton_nodes(graph: BaseGraph) -> None:
    """
    Remove singleton nodes (nodes that have a degree of 0) from the graph.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph

    """
    start = current_time_in_millis()
    singleton = []
    for n, d in graph.degree():
        if d == 0:
            singleton.append(n)
    while singleton:
        n = singleton.pop()
        log.debug(f"Removing singleton node {n}")
        graph.remove_node(n)
    end = current_time_in_millis()
    log.info(f"Time taken: {end - start} ms")
