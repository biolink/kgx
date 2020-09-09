import importlib
from typing import List, Tuple, Any, Optional, Dict, Set

import networkx
import pathlib

import kgx
from kgx import PandasTransformer, NeoTransformer
from kgx.config import get_logger
from kgx.utils.kgx_utils import current_time_in_millis

_transformers = {
    'tar': kgx.PandasTransformer,
    'csv': kgx.PandasTransformer,
    'tsv': kgx.PandasTransformer,
    'nt': kgx.NtTransformer,
    'ttl': kgx.RdfTransformer,
    'json': kgx.JsonTransformer,
    # 'rq': kgx.SparqlTransformer,
    'owl': kgx.RdfOwlTransformer,
    'rsa': kgx.RsaTransformer
}

log = get_logger()


def get_transformer(file_format: str) -> Any:
    """
    Get a Transformer corresponding to a given file format.

    .. note::
        This method returns a reference to kgx.Transformer class
        and not an instance of kgx.Transformer class.
        You will have to instantiate the class by calling its constructor.

    Parameters
    ----------
    file_format: str
        File format

    Returns
    -------
    Any
        Reference to kgx.Transformer class corresponding to ``file_format``

    """
    t = _transformers.get(file_format)
    if not t:
        raise TypeError(f"format '{file_format}' is not a supported file type.")
    return t


def get_file_types() -> Tuple:
    """
    Get all file formats supported by KGX.

    Returns
    -------
    Tuple
        A tuple of supported file formats

    """
    return tuple(_transformers.keys())


def parse_target(key: str, target: dict, output_directory: str, curie_map: Dict[str, str] = None, node_properties: Set[str] = None, predicate_mappings: Dict[str, str] = None):
    """
    Parse a target (source) from a merge config YAML.

    Parameters
    ----------
    key: str
        Target key
    target: Dict
        Target configuration
    output_directory:
        Location to write output to
    curie_map: Dict[str, str]
        Non-canonical CURIE mappings
    node_properties: Set[str]
        A set of predicates that ought to be treated as node properties (This is applicable for RDF)
    predicate_mappings: Dict[str, str]
        A mapping of predicate IRIs to property names (This is applicable for RDF)

    """
    target_name = target['name'] if 'name' in target else key
    log.info(f"Processing target '{key}'")
    target_curie_map = target['curie_map'] if 'curie_map' in target else {}
    target_curie_map.update(curie_map)
    target_predicate_mappings = target['predicate_mappings'] if 'predicate_mappings' in target else {}
    target_predicate_mappings.update(predicate_mappings)
    target_node_properties = target['node_properties'] if 'node_properties' in target else []
    target_node_properties.extend(node_properties)

    input_format = target['type']
    compression = target['compression'] if 'compression' in target else None

    if target['type'] in {'nt', 'ttl'}:
        # Parse RDF file types
        transformer = get_transformer(target['type'])(curie_map=target_curie_map)
        transformer.set_predicate_mapping(predicate_mappings)
        transformer.graph.name = key
        if 'filters' in target:
            apply_filters(target, transformer)
        for f in target['filename']:
            transformer.parse(
                filename=f,
                input_format=input_format,
                compression=compression,
                node_property_predicates=target_node_properties,
                provided_by=target_name
            )
        if 'operations' in target:
            apply_operations(target, transformer.graph)
    elif target['type'] in get_file_types():
        # Parse other supported file types
        transformer = get_transformer(target['type'])()
        transformer.graph.name = key
        if 'filters' in target:
            apply_filters(target, transformer)
        for f in target['filename']:
            transformer.parse(
                filename=f,
                input_format=input_format,
                compression=compression,
                provided_by=target_name
            )
        if 'operations' in target:
            apply_operations(target, transformer.graph)
    elif target['type'] == 'neo4j':
        # Parse Neo4j
        transformer = NeoTransformer(
            source_graph=None,
            uri=target['uri'],
            username=target['username'],
            password=target['password']
        )
        transformer.graph.name = key
        if 'filters' in target:
            apply_filters(target, transformer)
        transformer.load(provided_by=target_name)
        if 'operations' in target:
            apply_operations(target, transformer.graph)
        transformer.graph.name = key
    else:
        log.error("type {} not yet supported".format(target['type']))
    pt = PandasTransformer(transformer.graph)
    pt.save(filename=f'{output_directory}/{key}', output_format='tsv', compression=None)
    return transformer.graph


def apply_filters(target: dict, transformer: kgx.Transformer) -> kgx.Transformer:
    """
    Apply filters as defined in the YAML.

    Parameters
    ----------
    target: dict
        The target from the YAML
    transformer: kgx.Transformer
        The transformer corresponding to the target

    Returns
    -------
    transformer: kgx.Transformer
        The transformer corresponding to the target

    """
    filters = target['filters']
    node_filters = filters['node_filters'] if 'node_filters' in filters else {}
    edge_filters = filters['edge_filters'] if 'edge_filters' in filters else {}
    for k, v in node_filters.items():
        transformer.set_node_filter(k, set(v))
    for k, v in edge_filters.items():
        transformer.set_edge_filter(k, set(v))
    log.info(f"with node filters: {node_filters}")
    log.info(f"with edge filters: {edge_filters}")
    return transformer


def apply_operations(target: dict, graph: networkx.MultiDiGraph) -> networkx.MultiDiGraph:
    """
    Apply operations as defined in the YAML.

    Parameters
    ----------
    target: dict
        The target from the YAML
    graph: networkx.MultiDiGraph
        The graph corresponding to the target

    Returns
    -------
    networkx.MultiDiGraph
        The graph corresponding to the target

    """
    operations = target['operations']
    for operation in operations:
        op_name = operation['name']
        op_args = operation['args']
        module_name = '.'.join(op_name.split('.')[0:-1])
        function_name = op_name.split('.')[-1]
        f = getattr(importlib.import_module(module_name), function_name)
        log.info(f"Applying operation {op_name} with args: {op_args}")
        f(graph, **op_args)
    return graph
