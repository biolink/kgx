import logging
import networkx as nx
import rdflib
import stringcase

from kgx.mapper import get_prefix
from kgx.utils.kgx_utils import get_toolkit, get_curie_lookup_map

from kgx.validator import is_curie

ONTOLOGY_PREFIX_MAP = {}
ONTOLOGY_GRAPH_CACHE = {}


def get_parents(graph, node, relations=None):
    """
    Return all direct 'parents' of specified node, filtered by relations.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        Graph to traverse
    node: str
        node identifier
    relations: list[str]
       list of relations

    Returns
    -------
    list[str]
        A list of parent node(s)

    """
    parents = []
    logging.info("get parents for node: {}".format(node))

    if node in graph:
        out_edges = [x for x in graph.out_edges(node, data=True)]
        print("$$$ out_edges: {}".format(out_edges))
        if relations is None:
            parents = [x[1] for x in out_edges]
        else:
            parents = [x[1] for x in out_edges if x[2]['edge_label'] in relations]
    return parents


def get_ancestors(graph, node, relations=None):
    """
    Return all ancestors of specified node, filtered by relations.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        Graph to traverse
    node: str
        node identifier
    relations: list[str]
       list of relations

    Returns
    -------
    list[str]
        A list of ancestor nodes

    """
    logging.info("get ancestors for node: {}".format(node))
    seen = []
    nextnodes = [node]
    while len(nextnodes) > 0:
        nn = nextnodes.pop()
        if nn not in seen:
            seen.append(nn)
            nextnodes += get_parents(graph, nn, relations=relations)
    seen.remove(node)
    return seen

def get_category_via_superclass(graph, curie, load_ontology=True):
    """
    Get category for a given CURIE by tracing its superclass, via subclass_of hierarchy,
    and getting the most appropriate category based on the superclass.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        Graph to traverse
    curie: str
        Input CURIE
    load_ontology: bool
        Determines whether to load ontology, based on CURIE prefix, or to simply
        rely on subclass_of hierarchy from the graph

    Returns
    -------
    set
        A set containing one (or more) category for the given curie

    """
    from kgx.utils.kgx_utils import get_curie_lookup_service
    cls = get_curie_lookup_service()
    logging.debug("curie: {}".format(curie))
    new_categories = []
    toolkit = get_toolkit()
    if is_curie(curie):
        ancestors = get_ancestors(graph, curie, relations=['subclass_of'])
        if len(ancestors) == 0 and load_ontology:
            # load ontology
            prefix = get_prefix(curie)
            #ontology_graph = load_ontology_graph(prefix, prefix_map={'SO': 'data/so.owl'})
            ontology_graph = cls.ontology_graph
            new_categories += [x for x in get_category_via_superclass(ontology_graph, curie, False)]
        logging.debug("Ancestors for CURIE {} via subClassOf: {}".format(curie, ancestors))
        seen = []
        for anc in ancestors:
            mapping = toolkit.get_by_mapping(anc)
            seen.append(anc)
            if mapping:
                # there is direct mapping to BioLink Model
                logging.debug("Ancestor {} mapped to {}".format(anc, mapping))
                seen_labels = [graph.nodes[x]['name'] for x in seen]
                new_categories += [x for x in seen_labels]
                new_categories += [x for x in toolkit.ancestors(mapping)]
                break
    return set(new_categories)

def load_ontology_graph(prefix, prefix_map=None):
    """
    Load an ontology into a networkx.MultiDiGraph representation.

    Parameters
    ----------
    prefix: str
        Ontology prefix
    prefix_map: dict
        Dictionary containing prefix to ontology OWL mapping

    Returns
    -------
    networkx.MultiDiGraph
        The ontology graph

    """
    global ONTOLOGY_GRAPH_CACHE, ONTOLOGY_PREFIX_MAP
    if prefix_map is None:
        prefix_map = ONTOLOGY_PREFIX_MAP
    from kgx import RdfOwlTransformer
    ont = RdfOwlTransformer()
    if prefix in ONTOLOGY_GRAPH_CACHE:
        graph = ONTOLOGY_GRAPH_CACHE[prefix]
    else:
        ont.parse(prefix_map[prefix])
        ONTOLOGY_GRAPH_CACHE[prefix] = graph = ont.graph
    return graph

def curie_lookup(curie, graph=None):
    from kgx.utils.kgx_utils import get_curie_lookup_service
    cls = get_curie_lookup_service()
    name = None
    prefix = get_prefix(curie)
    curie_map = get_curie_lookup_map()
    if prefix in ['OIO', 'OWL', 'owl', 'OBO', 'rdfs']:
        name = stringcase.snakecase(curie.split(':', 1)[1])
        return name
    if curie in curie_map:
        name = curie_map[curie]
        return name
    if curie in cls.ontology_graph:
        name = g.nodes[curie]['name']

    return name