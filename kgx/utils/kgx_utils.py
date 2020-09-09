import re
import time
from typing import List, Dict, Set, Optional, Any, Union

import networkx
import stringcase
from biolinkml.meta import TypeDefinitionName, ElementName, SlotDefinition, ClassDefinition, TypeDefinition, Element
from bmt import Toolkit
from cachetools import LRUCache
from prefixcommons.curie_util import contract_uri
from prefixcommons.curie_util import expand_uri

from kgx.config import get_jsonld_context, get_logger

toolkit = None
curie_lookup_service = None
cache = None

log = get_logger()

is_property_multivalued = {
    'id': False,
    'subject': False,
    'object': False,
    'edge_label': False,
    'description': False,
    'synonym': True,
    'in_taxon': False,
    'same_as': True,
    'name': False,
    'has_evidence': False,
    'provided_by': True,
    'category': True,
    'publications': True,
    'type': False,
    'relation': False
}

CORE_NODE_PROPERTIES = {'id', 'name'}
CORE_EDGE_PROPERTIES = {'id', 'subject', 'edge_label', 'object', 'relation'}


def camelcase_to_sentencecase(s: str) -> str:
    """
    Convert CamelCase to sentence case.

    Parameters
    ----------
    s: str
        Input string in CamelCase

    Returns
    -------
    str
        string in sentence case form

    """
    return stringcase.sentencecase(s).lower()


def snakecase_to_sentencecase(s: str) -> str:
    """
    Convert snake_case to sentence case.

    Parameters
    ----------
    s: str
        Input string in snake_case

    Returns
    -------
    str
        string in sentence case form

    """
    return stringcase.sentencecase(s).lower()


def sentencecase_to_snakecase(s: str) -> str:
    """
    Convert sentence case to snake_case.

    Parameters
    ----------
    s: str
        Input string in sentence case

    Returns
    -------
    str
        string in snake_case form

    """
    return stringcase.snakecase(s).lower()


def sentencecase_to_camelcase(s: str) -> str:
    """
    Convert sentence case to CamelCase.

    Parameters
    ----------
    s: str
        Input string in sentence case

    Returns
    -------
    str
        string in CamelCase form

    """
    return stringcase.pascalcase(stringcase.snakecase(s))


def format_biolink_category(s: str) -> str:
    """
    Convert a sentence case Biolink category name to
    a proper Biolink CURIE with the category itself
    in CamelCase form.

    Parameters
    ----------
    s: str
        Input string in sentence case

    Returns
    -------
    str
        a proper Biolink CURIE
    """
    if re.match("biolink:.+", s):
        return s
    else:
        formatted = sentencecase_to_camelcase(s)
        return f"biolink:{formatted}"


def format_biolink_slots(s: str) -> str:
    if re.match("biolink:.+", s):
        return s
    else:
        formatted = sentencecase_to_snakecase(s)
        return f"biolink:{formatted}"


def contract(uri: str, prefix_maps: Optional[List[Dict]] = None, fallback: bool = True) -> str:
    """
    Contract a given URI to a CURIE, based on mappings from `prefix_maps`.
    If no prefix map is provided then will use defaults from prefixcommons-py.

    This method will return the URI as the CURIE if there is no mapping found.

    Parameters
    ----------
    uri: str
        A URI
    prefix_maps: Optional[List[Dict]]
        A list of prefix maps to use for mapping
    fallback: bool
        Determines whether to fallback to default prefix mappings, as determined
        by `prefixcommons.curie_util`, when URI prefix is not found in `prefix_maps`.

    Returns
    -------
    str
        A CURIE corresponding to the URI

    """
    curie = uri
    default_curie_maps = [get_jsonld_context('monarch_context'), get_jsonld_context('obo_context')]
    if prefix_maps:
        curie_list = contract_uri(uri, prefix_maps)
        if len(curie_list) == 0:
            if fallback:
                curie_list = contract_uri(uri, default_curie_maps)
                if curie_list:
                    curie = curie_list[0]
        else:
            curie = curie_list[0]
    else:
        curie_list = contract_uri(uri, default_curie_maps)
        if len(curie_list) > 0:
            curie = curie_list[0]

    return curie


def expand(curie: str, prefix_maps: Optional[List[dict]] = None, fallback: bool = True) -> str:
    """
    Expand a given CURIE to an URI, based on mappings from `prefix_map`.

    This method will return the CURIE as the IRI if there is no mapping found.

    Parameters
    ----------
    curie: str
        A CURIE
    prefix_maps: Optional[List[dict]]
        A list of prefix maps to use for mapping
    fallback: bool
        Determines whether to fallback to default prefix mappings, as determined
        by `prefixcommons.curie_util`, when CURIE prefix is not found in `prefix_maps`.

    Returns
    -------
    str
        A URI corresponding to the CURIE

    """
    default_curie_maps = [get_jsonld_context('monarch_context'), get_jsonld_context('obo_context')]
    if prefix_maps:
        uri = expand_uri(curie, prefix_maps)
        if uri == curie and fallback:
            uri = expand_uri(curie, default_curie_maps)
    else:
        uri = expand_uri(curie, default_curie_maps)

    return uri


def get_toolkit() -> Toolkit:
    """
    Get an instance of bmt.Toolkit
    If there no instance defined, then one is instantiated and returned.

    Returns
    -------
    bmt.Toolkit
        an instance of bmt.Toolkit

    """
    global toolkit
    if toolkit is None:
        toolkit = Toolkit()

    return toolkit


def generate_edge_key(s: str, edge_label: str, o: str) -> str:
    """
    Generates an edge key based on a given subject, edge_label and object.

    Parameters
    ----------
    s: str
        Subject
    edge_label: str
        Edge label
    o: str
        Object

    Returns
    -------
    str
        Edge key as a string

    """
    return '{}-{}-{}'.format(s, edge_label, o)


def get_biolink_mapping(category):
    """
    Get a BioLink Model mapping for a given ``category``.

    Parameters
    ----------
    category: str
        A category for which there is a mapping in BioLink Model

    Returns
    -------
    str
        A BioLink Model class corresponding to ``category``

    """
    # TODO: deprecate
    global toolkit
    element = toolkit.get_element(category)
    if element is None:
        element = toolkit.get_element(snakecase_to_sentencecase(category))
    return element


def get_curie_lookup_service():
    """
    Get an instance of kgx.curie_lookup_service.CurieLookupService

    Returns
    -------
    kgx.curie_lookup_service.CurieLookupService
        An instance of ``CurieLookupService``

    """
    global curie_lookup_service
    if curie_lookup_service is None:
        from kgx.curie_lookup_service import CurieLookupService
        curie_lookup_service = CurieLookupService()
    return curie_lookup_service


def get_cache(maxsize=10000):
    """
    Get an instance of cachetools.cache

    Parameters
    ----------
    maxsize: int
        The max size for the cache (``10000``, by default)

    Returns
    -------
    cachetools.cache
        An instance of cachetools.cache

    """
    global cache
    if cache is None:
        cache = LRUCache(maxsize)
    return cache


def current_time_in_millis():
    return int(round(time.time() * 1000))


def get_prefix_prioritization_map():
    toolkit = get_toolkit()
    prefix_prioritization_map = {}
    # TODO: Lookup via Biolink CURIE should be supported in bmt
    descendants = toolkit.descendents('named thing')
    descendants.append('named thing')
    for d in descendants:
        element = toolkit.get_element(d)
        if 'id_prefixes' in element:
            prefixes = element.id_prefixes
            key = format_biolink_category(element.name)
            prefix_prioritization_map[key] = prefixes
    return prefix_prioritization_map


def get_biolink_element(name):
    toolkit = get_toolkit()
    if re.match("biolink:.+", name):
        name = name.split(':', 1)[1]
        name = camelcase_to_sentencecase(name)

    element = toolkit.get_element(name)
    return element


def get_biolink_ancestors(name):
    toolkit = get_toolkit()
    if re.match("biolink:.+", name):
        name = name.split(':', 1)[1]
        name = camelcase_to_sentencecase(name)

    ancestors = toolkit.ancestors(name)
    formatted_ancestors = [format_biolink_category(x) for x in ancestors]
    return formatted_ancestors


def get_biolink_node_properties():
    toolkit = get_toolkit()
    properties = toolkit.children('node property')
    # TODO: fix bug in bmt when getting descendants
    node_properties = set()
    for p in properties:
        element = toolkit.get_element(p)
        node_properties.add(element.name)
    element = toolkit.get_element('category')
    node_properties.add(element.name)
    return set([format_biolink_slots(x) for x in node_properties])


def get_biolink_edge_properties():
    toolkit = get_toolkit()
    properties = toolkit.children('association slot')
    edge_properties = set()
    for p in properties:
        element = toolkit.get_element(p)
        edge_properties.add(element.name)

    return set([format_biolink_slots(x) for x in edge_properties])


def get_biolink_relations():
    toolkit = get_toolkit()
    relations = toolkit.descendents('related to')
    return relations


def get_biolink_property_types():
    toolkit = get_toolkit()
    types = {}
    node_properties = set()
    edge_properties = set()

    properties = toolkit.children('node property')
    for p in properties:
        element = toolkit.get_element(p)
        node_properties.add(element.name)

    properties = toolkit.children('association slot')
    for p in properties:
        element = toolkit.get_element(p)
        edge_properties.add(element.name)

    for p in node_properties:
        property_type = get_type_for_property(p)
        types[format_biolink_slots(p)] = property_type

    for p in edge_properties:
        property_type = get_type_for_property(p)
        types[format_biolink_slots(p)] = property_type

    # TODO: this should be moved to biolink model
    types['biolink:predicate'] = 'uriorcurie'
    types['biolink:edge_label'] = 'uriorcurie'

    return types

def get_type_for_property(p: str) -> str:
    """
    Get type for a property.

    TODO: Move this to biolink-model-toolkit

    Parameters
    ----------
    p: str

    Returns
    -------
    str
        The type for a given property

    """
    toolkit = get_toolkit()
    e = toolkit.get_element(p)
    if isinstance(e, ClassDefinition):
        t = "uriorcurie"
    elif isinstance(e, TypeDefinition):
        t = e.uri
    else:
        r = e.range
        if isinstance(r, SlotDefinition):
            t = r.range
            t = get_type_for_property(t)
        elif isinstance(r, TypeDefinitionName):
            t = get_type_for_property(r)
        elif isinstance(r, ElementName):
            t = get_type_for_property(r)
        else:
            t = "xsd:string"
    return t


def get_biolink_association_types():
    toolkit = get_toolkit()
    associations = toolkit.descendents('association')
    associations.append('association')
    formatted_associations = set([format_biolink_category(x) for x in associations])
    return formatted_associations


def prepare_data_dict(d1, d2, preserve = True):
    new_data = {}
    for key, value in d2.items():
        if isinstance(value, (list, set, tuple)):
            new_value = [x for x in value]
        else:
            new_value = value

        if key in is_property_multivalued:
            if is_property_multivalued[key]:
                # value for key is supposed to be multivalued
                if key in d1:
                    # key is in data
                    if isinstance(d1[key], (list, set, tuple)):
                        # existing key has value type list
                        new_data[key] = d1[key]
                        if isinstance(new_value, (list, set, tuple)):
                            new_data[key] += [x for x in new_value if x not in new_data[key]]
                        else:
                            if new_value not in new_data[key]:
                                new_data[key].append(new_value)
                    else:
                        if key in CORE_NODE_PROPERTIES or key in CORE_EDGE_PROPERTIES:
                            log.debug(f"cannot modify core property '{key}': {d2[key]} vs {d1[key]}")
                        else:
                            # existing key does not have value type list; converting to list
                            new_data[key] = [d1[key]]
                            if isinstance(new_value, (list, set, tuple)):
                                new_data[key] += [x for x in new_value if x not in new_data[key]]
                            else:
                                if new_value not in new_data[key]:
                                    new_data[key].append(new_value)
                else:
                    # key is not in data; adding
                    if isinstance(new_value, (list, set, tuple)):
                        new_data[key] = [x for x in new_value]
                    else:
                        new_data[key] = [new_value]
            else:
                # key is not multivalued; adding/replacing as-is
                if key in d1:
                    if isinstance(d1[key], (list, set, tuple)):
                        new_data[key] = d1[key]
                        if isinstance(new_value, (list, set, tuple)):
                            new_data[key] += [x for x in new_value]
                        else:
                            new_data[key].append(new_value)
                    else:
                        if key in CORE_NODE_PROPERTIES or key in CORE_EDGE_PROPERTIES:
                            log.debug(f"cannot modify core property '{key}': {d2[key]} vs {d1[key]}")
                        else:
                            if preserve:
                                new_data[key] = [d1[key]]
                                if isinstance(new_value, (list, set, tuple)):
                                    new_data[key] += [x for x in new_value if x not in new_data[key]]
                                else:
                                    new_data[key].append(new_value)
                            else:
                                new_data[key] = new_value
                else:
                    new_data[key] = new_value
        else:
            # treating key as multivalued
            if key in d1:
                # key is in data
                if key in CORE_NODE_PROPERTIES or key in CORE_EDGE_PROPERTIES:
                    log.debug(f"cannot modify core property '{key}': {d2[key]} vs {d1[key]}")
                else:
                    if isinstance(d1[key], (list, set, tuple)):
                        # existing key has value type list
                        new_data[key] = d1[key]
                        if isinstance(new_value, (list, set, tuple)):
                            new_data[key] += [x for x in new_value if x not in new_data[key]]
                        else:
                            new_data[key].append(new_value)
                    else:
                        # existing key does not have value type list; converting to list
                        if preserve:
                            new_data[key] = [d1[key]]
                            if isinstance(new_value, (list, set, tuple)):
                                new_data[key] += [x for x in new_value if x not in new_data[key]]
                            else:
                                new_data[key].append(new_value)
                        else:
                            new_data[key] = new_value
            else:
                new_data[key] = new_value
    return new_data


def apply_filters(graph: networkx.MultiDiGraph, node_filters: Dict[str, Union[str, Set]], edge_filters: Dict[str, Union[str, Set]]) -> None:
    """
    Apply filters to graph and remove nodes and edges that
    do not pass given filters.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        The graph
    node_filters: Dict[str, Union[str, Set]]
        Node filters
    edge_filters: Dict[str, Union[str, Set]]
        Edge filters

    """
    apply_node_filters(graph, node_filters)
    apply_edge_filters(graph, edge_filters)


def apply_node_filters(graph: networkx.MultiDiGraph, node_filters: Dict[str, Union[str, Set]]) -> None:
    """
    Apply filters to graph and remove nodes that do not pass given filters.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        The graph
    node_filters: Dict[str, Union[str, Set]]
        Node filters

    """
    nodes_to_remove = []
    for node, node_data in graph.nodes(data=True):
        pass_filter = True
        for k, v in node_filters.items():
            if k == 'category':
                if not any(x in node_data[k] for x in v):
                    pass_filter = False
        if not pass_filter:
            nodes_to_remove.append(node)

    for node in nodes_to_remove:
        # removing node that fails category filter
        log.debug(f"Removing node {node}")
        graph.remove_node(node)


def apply_edge_filters(graph: networkx.MultiDiGraph, edge_filters: Dict[str, Union[str, Set]]) -> None:
    """
    Apply filters to graph and remove edges that do not pass given filters.

    Parameters
    ----------
    graph: networkx.MultiDiGraph
        The graph
    edge_filters: Dict[str, Union[str, Set]]
        Edge filters

    """
    edges_to_remove = []
    for subject_node, object_node, key, data in graph.edges(keys=True, data=True):
        pass_filter = True
        for k, v in edge_filters.items():
            if k == 'edge_label':
                if data[k] not in v:
                    pass_filter = False
            elif k == 'relation':
                if data[k] not in v:
                    pass_filter = False
        if not pass_filter:
            edges_to_remove.append((subject_node, object_node, key))

    for edge in edges_to_remove:
        # removing edge that fails edge filters
        log.debug(f"Removing edge {edge}")
        graph.remove_edge(edge[0], edge[1], edge[2])
