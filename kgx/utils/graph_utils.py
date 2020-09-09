from typing import List, Set, Dict, Optional
import networkx as nx
import stringcase
from cachetools import cached

from kgx.config import get_logger
from kgx.utils.kgx_utils import get_toolkit, get_cache, get_curie_lookup_service, generate_edge_key, CORE_NODE_PROPERTIES, CORE_EDGE_PROPERTIES
from kgx.prefix_manager import PrefixManager

ONTOLOGY_PREFIX_MAP: Dict = {}
ONTOLOGY_GRAPH_CACHE: Dict = {}

log = get_logger()


def get_parents(graph: nx.MultiDiGraph, node: str, relations: List[str] = None) -> List[str]:
    """
    Return all direct `parents` of a specified node, filtered by ``relations``.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
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
    if node in graph:
        out_edges = [x for x in graph.out_edges(node, data=True)]
        if relations is None:
            parents = [x[1] for x in out_edges]
        else:
            parents = [x[1] for x in out_edges if x[2]['edge_label'] in relations]
    return parents


def get_ancestors(graph: nx.MultiDiGraph, node: str, relations: List[str] = None) -> List[str]:
    """
    Return all `ancestors` of specified node, filtered by ``relations``.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
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
def get_category_via_superclass(graph: nx.MultiDiGraph, curie: str, load_ontology: bool = True) -> Set[str]:
    """
    Get category for a given CURIE by tracing its superclass, via ``subclass_of`` hierarchy,
    and getting the most appropriate category based on the superclass.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
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
                seen_labels = [graph.nodes[x]['name'] for x in seen if 'name' in graph.nodes[x]]
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
        name = cls.ontology_graph.nodes[curie]['name']
    return name


def remap_node_identifier(graph: nx.MultiDiGraph, category: str, alternative_property: str, prefix=None) -> nx.MultiDiGraph:
    """
    Remap a node's 'id' attribute with value from a node's ``alternative_property`` attribute.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
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
    networkx.MultiDiGraph
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
                            mapping[nid] = v
                            break
                else:
                    # no prefix defined; pick the 1st one from list
                    mapping[nid] = next(iter(alternative_values))
            elif isinstance(alternative_values, str):
                if prefix:
                    if alternative_values.startswith(prefix):
                        mapping[nid] = alternative_values
                else:
                    # no prefix defined
                    mapping[nid] = alternative_values
            else:
                log.error(f"Cannot use {alternative_values} from alternative_property {alternative_property}")

    nx.set_node_attributes(graph, values=mapping, name='id')
    nx.relabel_nodes(graph, mapping, copy=False)

    # update 'subject' of all outgoing edges
    update_edge_keys = {}
    updated_subject_values = {}
    updated_object_values = {}
    for u, v, k, edge_data in graph.edges(keys=True, data=True):
        if u is not edge_data['subject']:
            updated_subject_values[(u, v, k)] = u
            update_edge_keys[(u, v, k)] = generate_edge_key(u, edge_data['edge_label'], v)
        if v is not edge_data['object']:
            updated_object_values[(u, v, k)] = v
            update_edge_keys[(u, v, k)] = generate_edge_key(u, edge_data['edge_label'], v)

    nx.set_edge_attributes(graph, values=updated_subject_values, name='subject')
    nx.set_edge_attributes(graph, values=updated_object_values, name='object')
    nx.set_edge_attributes(graph, values=update_edge_keys, name='edge_key')

    return graph


def remap_node_property(graph: nx.MultiDiGraph, category: str, old_property: str, new_property: str) -> None:
    """
    Remap the value in node ``old_property`` attribute with value
    from node ``new_property`` attribute.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
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
            mapping[nid] = node_data[new_property]
    nx.set_node_attributes(graph, values=mapping, name=old_property)


def remap_edge_property(graph: nx.MultiDiGraph, edge_label: str, old_property: str, new_property: str) -> None:
    """
    Remap the value in an edge ``old_property`` attribute with value
    from edge ``new_property`` attribute.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        The graph
    edge_label: string
        edge_label referring to edges whose property needs to be remapped
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
        if edge_label is not edge_data['edge_label']:
            continue
        if new_property in edge_data:
            mapping[(u, v, k)] = edge_data[new_property]
    nx.set_edge_attributes(graph, values=mapping, name=old_property)
