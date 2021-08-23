from typing import List, Set, Dict, Optional
import stringcase
from cachetools import cached

from kgx.config import get_logger
from kgx.graph.base_graph import BaseGraph
from kgx.utils.kgx_utils import get_toolkit, get_cache, get_curie_lookup_service
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
            parents = [x[1] for x in out_edges if x[2]["predicate"] in relations]
    return parents


def get_ancestors(
    graph: BaseGraph, node: str, relations: List[str] = None
) -> List[str]:
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
def get_category_via_superclass(
    graph: BaseGraph, curie: str, load_ontology: bool = True
) -> Set[str]:
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
        ancestors = get_ancestors(graph, curie, relations=["subclass_of"])
        if len(ancestors) == 0 and load_ontology:
            cls = get_curie_lookup_service()
            ontology_graph = cls.ontology_graph
            new_categories += [
                x for x in get_category_via_superclass(ontology_graph, curie, False)
            ]
        log.debug("Ancestors for CURIE {} via subClassOf: {}".format(curie, ancestors))
        seen = []
        for anc in ancestors:
            mapping = toolkit.get_by_mapping(anc)
            seen.append(anc)
            if mapping:
                # there is direct mapping to BioLink Model
                log.debug("Ancestor {} mapped to {}".format(anc, mapping))
                seen_labels = [
                    graph.nodes()[x]["name"] for x in seen if "name" in graph.nodes()[x]
                ]
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
    if prefix in ["OIO", "OWL", "owl", "OBO", "rdfs"]:
        name = stringcase.snakecase(curie.split(":", 1)[1])
    elif curie in cls.curie_map:
        name = cls.curie_map[curie]
    elif curie in cls.ontology_graph:
        name = cls.ontology_graph.nodes()[curie]["name"]
    return name
