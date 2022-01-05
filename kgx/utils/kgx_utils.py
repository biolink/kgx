import importlib
import re
import time
import uuid
from enum import Enum
from typing import List, Dict, Set, Optional, Any, Union
import stringcase
from linkml_runtime.linkml_model.meta import (
    TypeDefinitionName,
    ElementName,
    SlotDefinition,
    ClassDefinition,
    TypeDefinition,
    Element,
)
from bmt import Toolkit
from cachetools import LRUCache
import pandas as pd
import numpy as np
from prefixcommons.curie_util import contract_uri
from prefixcommons.curie_util import expand_uri

from kgx.config import get_logger, get_jsonld_context, get_biolink_model_schema
from kgx.graph.base_graph import BaseGraph

curie_lookup_service = None
cache = None

log = get_logger()

DEFAULT_NODE_CATEGORY = "biolink:NamedThing"
DEFAULT_EDGE_PREDICATE = "biolink:related_to"
CORE_NODE_PROPERTIES = {"id", "name"}
CORE_EDGE_PROPERTIES = {"id", "subject", "predicate", "object", "type"}

LIST_DELIMITER = "|"


class GraphEntityType(Enum):
    GRAPH = "graph"
    NODE = "node"
    EDGE = "edge"


# Biolink 2.0 "Knowledge Source" association slots,
# including the deprecated 'provided_by' slot

provenance_slot_types = {
    "knowledge_source": list,
    "primary_knowledge_source": str,
    "original_knowledge_source": str,
    "aggregator_knowledge_source": list,
    "supporting_data_source": list,
    "provided_by": list,
}

column_types = {
    "publications": list,
    "qualifiers": list,
    "category": list,
    "synonym": list,
    "same_as": list,
    "negated": bool,
    "xrefs": list,
}

column_types.update(provenance_slot_types)

knowledge_provenance_properties = set(provenance_slot_types.keys())

extension_types = {"csv": ",", "tsv": "\t", "csv:neo4j": ",", "tsv:neo4j": "\t"}

archive_read_mode = {"tar": "r", "tar.gz": "r:gz", "tar.bz2": "r:bz2"}
archive_write_mode = {"tar": "w", "tar.gz": "w:gz", "tar.bz2": "w:bz2"}

archive_format = {
    "r": "tar",
    "r:gz": "tar.gz",
    "r:bz2": "tar.bz2",
    "w": "tar",
    "w:gz": "tar.gz",
    "w:bz2": "tar.bz2",
}

is_provenance_property_multivalued = {
    "knowledge_source": True,
    "primary_knowledge_source": False,
    "original_knowledge_source": False,
    "aggregator_knowledge_source": True,
    "supporting_data_source": True,
    "provided_by": True,
}

is_property_multivalued = {
    "id": False,
    "subject": False,
    "object": False,
    "predicate": False,
    "description": False,
    "synonym": True,
    "in_taxon": False,
    "same_as": True,
    "name": False,
    "has_evidence": False,
    "category": True,
    "publications": True,
    "type": False,
    "relation": False,
}

is_property_multivalued.update(is_provenance_property_multivalued)


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


def contract(
    uri: str, prefix_maps: Optional[List[Dict]] = None, fallback: bool = True
) -> str:
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
    default_curie_maps = [
        get_jsonld_context("monarch_context"),
        get_jsonld_context("obo_context"),
    ]
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


def expand(
    curie: str, prefix_maps: Optional[List[dict]] = None, fallback: bool = True
) -> str:
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
    default_curie_maps = [
        get_jsonld_context("monarch_context"),
        get_jsonld_context("obo_context"),
    ]
    if prefix_maps:
        uri = expand_uri(curie, prefix_maps)
        if uri == curie and fallback:
            uri = expand_uri(curie, default_curie_maps)
    else:
        uri = expand_uri(curie, default_curie_maps)

    return uri


_default_toolkit = None

# TODO: not sure how threadsafe this simple-minded Toolkit cache is
_toolkit_versions: Dict[str, Toolkit] = dict()


def get_toolkit(biolink_release: Optional[str] = None) -> Toolkit:
    """
    Get an instance of bmt.Toolkit
    If there no instance defined, then one is instantiated and returned.

    Parameters
    ----------
    biolink_release: Optional[str]
        URL to (Biolink) Model Schema to be used for validated (default: None, use default Biolink Model Toolkit schema)

    """
    global _default_toolkit, _toolkit_versions
    if biolink_release:
        if biolink_release in _toolkit_versions:
            toolkit = _toolkit_versions[biolink_release]
        else:
            schema = get_biolink_model_schema(biolink_release)
            toolkit = Toolkit(schema=schema)
            _toolkit_versions[biolink_release] = toolkit
    else:
        if _default_toolkit is None:
            _default_toolkit = Toolkit()
        toolkit = _default_toolkit
        biolink_release = toolkit.get_model_version()
        if biolink_release not in _toolkit_versions:
            _toolkit_versions[biolink_release] = toolkit

    return toolkit


def generate_edge_key(s: str, edge_predicate: str, o: str) -> str:
    """
    Generates an edge key based on a given subject, predicate, and object.

    Parameters
    ----------
    s: str
        Subject
    edge_predicate: str
        Edge label
    o: str
        Object

    Returns
    -------
    str
        Edge key as a string

    """
    return "{}-{}-{}".format(s, edge_predicate, o)


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
    """
    Get current time in milliseconds.

    Returns
    -------
    int
        Time in milliseconds

    """
    return int(round(time.time() * 1000))


def get_prefix_prioritization_map() -> Dict[str, List]:
    """
    Get prefix prioritization map as defined in Biolink Model.

    Returns
    -------
    Dict[str, List]

    """
    toolkit = get_toolkit()
    prefix_prioritization_map = {}
    # TODO: Lookup via Biolink CURIE should be supported in bmt
    descendants = toolkit.get_descendants("named thing")
    descendants.append("named thing")
    for d in descendants:
        element = toolkit.get_element(d)
        if element and "id_prefixes" in element:
            prefixes = element.id_prefixes
            key = format_biolink_category(element.name)
            prefix_prioritization_map[key] = prefixes
    return prefix_prioritization_map


def get_biolink_element(name) -> Optional[Element]:
    """
    Get Biolink element for a given name, where name can be a class, slot, or relation.

    Parameters
    ----------
    name: str
        The name

    Returns
    -------
    Optional[linkml_model.meta.Element]
        An instance of linkml_model.meta.Element

    """
    toolkit = get_toolkit()
    element = toolkit.get_element(name)
    return element


def get_biolink_ancestors(name: str):
    """
    Get ancestors for a given Biolink class.

    Parameters
    ----------
    name: str

    Returns
    -------
    List
        A list of ancestors

    """
    toolkit = get_toolkit()
    ancestors_mixins = toolkit.get_ancestors(name, formatted=True, mixin=True)
    return ancestors_mixins


def get_biolink_property_types() -> Dict:
    """
    Get all Biolink property types.
    This includes both node and edges properties.

    Returns
    -------
    Dict
        A dict containing all Biolink property and their types

    """
    toolkit = get_toolkit()
    types = {}
    node_properties = toolkit.get_all_node_properties(formatted=True)
    edge_properties = toolkit.get_all_edge_properties(formatted=True)

    for p in node_properties:
        property_type = get_type_for_property(p)
        types[p] = property_type

    for p in edge_properties:
        property_type = get_type_for_property(p)
        types[p] = property_type

    # TODO: this should be moved to biolink model
    types["biolink:predicate"] = "uriorcurie"
    types["biolink:edge_label"] = "uriorcurie"
    return types


def get_type_for_property(p: str) -> str:
    """
    Get type for a property.

    TODO: Move this to biolink-model-default_toolkit

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
    t = "xsd:string"
    if e:
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


def prepare_data_dict(d1: Dict, d2: Dict, preserve: bool = True) -> Dict:
    """
    Given two dict objects, make a new dict object that is the intersection of the two.

    If a key is known to be multivalued then it's value is converted to a list.
    If a key is already multivalued then it is updated with new values.
    If a key is single valued, and a new unique value is found then the existing value is
    converted to a list and the new value is appended to this list.

    Parameters
    ----------
    d1: Dict
        Dict object
    d2: Dict
        Dict object
    preserve: bool
        Whether or not to preserve values for conflicting keys

    Returns
    -------
    Dict
        The intersection of d1 and d2

    """
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
                            new_data[key] += [
                                x for x in new_value if x not in new_data[key]
                            ]
                        else:
                            if new_value not in new_data[key]:
                                new_data[key].append(new_value)
                    else:
                        if key in CORE_NODE_PROPERTIES or key in CORE_EDGE_PROPERTIES:
                            log.debug(
                                f"cannot modify core property '{key}': {d2[key]} vs {d1[key]}"
                            )
                        else:
                            # existing key does not have value type list; converting to list
                            new_data[key] = [d1[key]]
                            if isinstance(new_value, (list, set, tuple)):
                                new_data[key] += [
                                    x for x in new_value if x not in new_data[key]
                                ]
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
                            log.debug(
                                f"cannot modify core property '{key}': {d2[key]} vs {d1[key]}"
                            )
                        else:
                            if preserve:
                                new_data[key] = [d1[key]]
                                if isinstance(new_value, (list, set, tuple)):
                                    new_data[key] += [
                                        x for x in new_value if x not in new_data[key]
                                    ]
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
                    log.debug(
                        f"cannot modify core property '{key}': {d2[key]} vs {d1[key]}"
                    )
                else:
                    if isinstance(d1[key], (list, set, tuple)):
                        # existing key has value type list
                        new_data[key] = d1[key]
                        if isinstance(new_value, (list, set, tuple)):
                            new_data[key] += [
                                x for x in new_value if x not in new_data[key]
                            ]
                        else:
                            new_data[key].append(new_value)
                    else:
                        # existing key does not have value type list; converting to list
                        if preserve:
                            new_data[key] = [d1[key]]
                            if isinstance(new_value, (list, set, tuple)):
                                new_data[key] += [
                                    x for x in new_value if x not in new_data[key]
                                ]
                            else:
                                new_data[key].append(new_value)
                        else:
                            new_data[key] = new_value
            else:
                new_data[key] = new_value

    for key, value in d1.items():
        if key not in new_data:
            new_data[key] = value
    return new_data


def apply_filters(
    graph: BaseGraph,
    node_filters: Dict[str, Union[str, Set]],
    edge_filters: Dict[str, Union[str, Set]],
) -> None:
    """
    Apply filters to graph and remove nodes and edges that
    do not pass given filters.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    node_filters: Dict[str, Union[str, Set]]
        Node filters
    edge_filters: Dict[str, Union[str, Set]]
        Edge filters

    """
    apply_node_filters(graph, node_filters)
    apply_edge_filters(graph, edge_filters)


def apply_node_filters(
    graph: BaseGraph, node_filters: Dict[str, Union[str, Set]]
) -> None:
    """
    Apply filters to graph and remove nodes that do not pass given filters.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    node_filters: Dict[str, Union[str, Set]]
        Node filters

    """
    nodes_to_remove = []
    for node, node_data in graph.nodes(data=True):
        pass_filter = True
        for k, v in node_filters.items():
            if k == "category":
                if not any(x in node_data[k] for x in v):
                    pass_filter = False
        if not pass_filter:
            nodes_to_remove.append(node)

    for node in nodes_to_remove:
        # removing node that fails category filter
        log.debug(f"Removing node {node}")
        graph.remove_node(node)


def apply_edge_filters(
    graph: BaseGraph, edge_filters: Dict[str, Union[str, Set]]
) -> None:
    """
    Apply filters to graph and remove edges that do not pass given filters.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        The graph
    edge_filters: Dict[str, Union[str, Set]]
        Edge filters

    """
    edges_to_remove = []
    for subject_node, object_node, key, data in graph.edges(keys=True, data=True):
        pass_filter = True
        for k, v in edge_filters.items():
            if k == "predicate":
                if data[k] not in v:
                    pass_filter = False
            elif k == "relation":
                if data[k] not in v:
                    pass_filter = False
        if not pass_filter:
            edges_to_remove.append((subject_node, object_node, key))

    for edge in edges_to_remove:
        # removing edge that fails edge filters
        log.debug(f"Removing edge {edge}")
        graph.remove_edge(edge[0], edge[1], edge[2])


def validate_node(node: Dict) -> Dict:
    """
    Given a node as a dictionary, check for required properties.
    This method will return the node dictionary with default
    assumptions applied, if any.

    Parameters
    ----------
    node: Dict
        A node represented as a dict

    Returns
    -------
    Dict
        A node represented as a dict, with default assumptions applied.

    """
    if len(node) == 0:
        log.debug(f"Empty node encountered: {node}")
    else:
        if "id" not in node:
            raise KeyError(f"node does not have 'id' property: {node}")
        if "name" not in node:
            log.debug(f"node does not have 'name' property: {node}")
        if "category" not in node:
            log.debug(
                f"node does not have 'category' property: {node}\nUsing {DEFAULT_NODE_CATEGORY} as default"
            )
            node["category"] = [DEFAULT_NODE_CATEGORY]
    return node


def validate_edge(edge: Dict) -> Dict:
    """
    Given an edge as a dictionary, check for required properties.
    This method will return the edge dictionary with default
    assumptions applied, if any.

    Parameters
    ----------
    edge: Dict
        An edge represented as a dict

    Returns
    -------
    Dict
        An edge represented as a dict, with default assumptions applied.
    """
    if "subject" not in edge:
        raise KeyError(f"edge does not have 'subject' property: {edge}")
    if "predicate" not in edge:
        raise KeyError(f"edge does not have 'predicate' property: {edge}")
    if "object" not in edge:
        raise KeyError(f"edge does not have 'object' property: {edge}")
    return edge


def generate_uuid():
    """
    Generates a UUID.

    Returns
    -------
    str
        A UUID

    """
    return f"urn:uuid:{uuid.uuid4()}"


def generate_edge_identifiers(graph: BaseGraph):
    """
    Generate unique identifiers for edges in a graph that do not
    have an ``id`` field.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph

    """
    for u, v, data in graph.edges(data=True):
        if "id" not in data:
            data["id"] = generate_uuid()


def sanitize_import(data: Dict) -> Dict:
    """
    Sanitize key-value pairs in dictionary.

    Parameters
    ----------
    data: Dict
        A dictionary containing key-value pairs

    Returns
    -------
    Dict
        A dictionary containing processed key-value pairs

    """
    tidy_data = {}
    for key, value in data.items():
        new_value = remove_null(value)
        if new_value is not None:
            tidy_data[key] = _sanitize_import(key, new_value)
    return tidy_data


def _sanitize_import(key: str, value: Any) -> Any:
    """
    Sanitize value for a key for the purpose of import.

    Parameters
    ----------
    key: str
        Key corresponding to a node/edge property
    value: Any
        Value corresponding to the key

    Returns
    -------
    value: Any
        Sanitized value

    """
    new_value: Any
    if key in column_types:
        if column_types[key] == list:
            if isinstance(value, (list, set, tuple)):
                value = [
                    v.replace("\n", " ").replace("\t", " ") if isinstance(v, str) else v
                    for v in value
                ]
                new_value = list(value)
            elif isinstance(value, str):
                value = value.replace("\n", " ").replace("\t", " ")
                new_value = [x for x in value.split(LIST_DELIMITER) if x]
            else:
                new_value = [str(value).replace("\n", " ").replace("\t", " ")]
        elif column_types[key] == bool:
            try:
                new_value = bool(value)
            except:
                new_value = False
        elif isinstance(value, (str, float)):
            new_value = value
        else:
            new_value = str(value).replace("\n", " ").replace("\t", " ")
    else:
        if isinstance(value, (list, set, tuple)):
            value = [
                v.replace("\n", " ").replace("\t", " ") if isinstance(v, str) else v
                for v in value
            ]
            new_value = list(value)
        elif isinstance(value, str):
            if LIST_DELIMITER in value:
                value = value.replace("\n", " ").replace("\t", " ")
                new_value = [x for x in value.split(LIST_DELIMITER) if x]
            else:
                new_value = value.replace("\n", " ").replace("\t", " ")
        elif isinstance(value, bool):
            try:
                new_value = bool(value)
            except:
                new_value = False
        elif isinstance(value, (str, float)):
            new_value = value
        else:
            new_value = str(value).replace("\n", " ").replace("\t", " ")
    return new_value


def _sanitize_export(key: str, value: Any) -> Any:
    """
    Sanitize value for a key for the purpose of export.

    Parameters
    ----------
    key: str
        Key corresponding to a node/edge property
    value: Any
        Value corresponding to the key

    Returns
    -------
    value: Any
        Sanitized value

    """
    new_value: Any
    if key in column_types:
        if column_types[key] == list:
            if isinstance(value, (list, set, tuple)):
                value = [
                    v.replace("\n", " ").replace('\\"', "").replace("\t", " ")
                    if isinstance(v, str)
                    else v
                    for v in value
                ]
                new_value = LIST_DELIMITER.join([str(x) for x in value])
            else:
                new_value = (
                    str(value).replace("\n", " ").replace('\\"', "").replace("\t", " ")
                )
        elif column_types[key] == bool:
            try:
                new_value = bool(value)
            except:
                new_value = False
        else:
            new_value = (
                str(value).replace("\n", " ").replace('\\"', "").replace("\t", " ")
            )
    else:
        if type(value) == list:
            new_value = LIST_DELIMITER.join([str(x) for x in value])
            new_value = (
                new_value.replace("\n", " ").replace('\\"', "").replace("\t", " ")
            )
            column_types[key] = list
        elif type(value) == bool:
            try:
                new_value = bool(value)
                column_types[key] = bool
            except:
                new_value = False
        else:
            new_value = (
                str(value).replace("\n", " ").replace('\\"', "").replace("\t", " ")
            )
    return new_value


def _build_export_row(data: Dict) -> Dict:
    """
    Casts all values to primitive types like str or bool according to the
    specified type in ``_column_types``. Lists become pipe delimited strings.

    Parameters
    ----------
    data: Dict
        A dictionary containing key-value pairs

    Returns
    -------
    Dict
        A dictionary containing processed key-value pairs

    """
    tidy_data = {}
    for key, value in data.items():
        new_value = remove_null(value)
        if new_value:
            tidy_data[key] = _sanitize_export(key, new_value)
    return tidy_data


def remove_null(input: Any) -> Any:
    """
    Remove any null values from input.

    Parameters
    ----------
    input: Any
        Can be a str, list or dict

    Returns
    -------
    Any
        The input without any null values

    """
    new_value: Any = None
    if isinstance(input, (list, set, tuple)):
        # value is a list, set or a tuple
        new_value = []
        for v in input:
            x = remove_null(v)
            if x:
                new_value.append(x)
    elif isinstance(input, dict):
        # value is a dict
        new_value = {}
        for k, v in input.items():
            x = remove_null(v)
            if x:
                new_value[k] = x
    elif isinstance(input, str):
        # value is a str
        if not is_null(input):
            new_value = input
    else:
        if not is_null(input):
            new_value = input
    return new_value


def is_null(item: Any) -> bool:
    """
    Checks if a given item is null or correspond to null.

    This method checks for: ``None``, ``numpy.nan``, ``pandas.NA``,
    ``pandas.NaT``, and ` `

    Parameters
    ----------
    item: Any
        The item to check

    Returns
    -------
    bool
        Whether the given item is null or not

    """
    null_values = {np.nan, pd.NA, pd.NaT, None, "", " "}
    return item in null_values


def apply_graph_operations(graph: BaseGraph, operations: List) -> None:
    """
    Apply graph operations to a given graph.

    Parameters
    ----------
    graph: kgx.graph.base_graph.BaseGraph
        An instance of BaseGraph
    operations: List
        A list of graph operations with configuration

    """
    for operation in operations:
        op_name = operation["name"]
        op_args = operation["args"]
        module_name = ".".join(op_name.split(".")[0:-1])
        function_name = op_name.split(".")[-1]
        f = getattr(importlib.import_module(module_name), function_name)
        f(graph, **op_args)
