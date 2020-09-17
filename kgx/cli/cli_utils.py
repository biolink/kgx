import importlib
import os
import sys
from multiprocessing import Pool
from typing import List, Tuple, Any, Optional, Dict, Set

import networkx
import yaml

import kgx
from kgx import PandasTransformer, NeoTransformer, Validator, RdfTransformer
from kgx.config import get_logger
from kgx.operations.graph_merge import merge_all_graphs
from kgx.operations.summarize_graph import summarize_graph

_transformers = {
    'tar': kgx.PandasTransformer,
    'csv': kgx.PandasTransformer,
    'tsv': kgx.PandasTransformer,
    'tsv:neo4j': kgx.PandasTransformer,
    'nt': kgx.NtTransformer,
    'ttl': kgx.RdfTransformer,
    'json': kgx.JsonTransformer,
    'obojson': kgx.ObographJsonTransformer,
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


def graph_summary(inputs: List[str], input_format: str, input_compression: str, output: str) -> Dict:
    """
    Loads and summarizes a knowledge graph from a set of input files.

    Parameters
    ----------
    inputs: List[str]
        Input file
    input_format: str
        Input file format
    input_compression: str
        The input compression type
    output: str
        Where to write the output (stdout, by default)

    Returns
    -------
    Dict
        A dictionary with the graph stats

    """
    transformer = get_transformer(input_format)()
    for file in inputs:
        transformer.parse(file, input_format=input_format, compression=input_compression)

    stats = summarize_graph(transformer.graph)
    if output:
        WH = open(output, 'w')
        WH.write(yaml.dump(stats))
    else:
        print(yaml.dump(stats))
    return stats


def validate(inputs: List[str], input_format: str, input_compression: str, output: str) -> kgx.Validator:
    """
    Run KGX validator on an input file to check for Biolink Model compliance.

    Parameters
    ----------
    inputs: List[str]
        Input files
    input_format: str
        The input format
    input_compression: str
        The input compression type
    output: str
        Path to output file

    Returns
    -------
    kgx.Validator
        An instance of the Validator

    """
    transformer = get_transformer(input_format)()
    for file in inputs:
        transformer.parse(file, input_format=input_format, compression=input_compression)

    validator = Validator()
    errors = validator.validate(transformer.graph)
    if output:
        validator.write_report(errors, open(output, 'w'))
    else:
        validator.write_report(errors, sys.stdout)
    return validator


def neo4j_download(uri: str, username: str, password: str, output: str, output_format: str, output_compression: str, node_filters: Optional[Tuple] = None, edge_filters: Optional[Tuple] = None) -> kgx.Transformer:
    """
    Download nodes and edges from Neo4j database.

    Parameters
    ----------
    uri: str
        Neo4j URI. For example, https://localhost:7474
    username: str
        Username for authentication
    password: str
        Password for authentication
    output: str
        Where to write the output (stdout, by default)
    output_format: str
        The output type (``csv``, by default)
    output_compression: str
        The output compression type
    node_filters: Optional[Tuple]
        Node filters
    edge_filters: Optional[Tuple]
        Edge filters

    Returns
    -------
    kgx.Transformer
        The NeoTransformer

    """
    transformer = NeoTransformer(uri=uri, username=username, password=password)
    if node_filters:
        for n in node_filters:
            transformer.set_node_filter(n[0], n[1])
    if edge_filters:
        for e in edge_filters:
            transformer.set_edge_filter(e[0], e[1])
    transformer.load()

    output_transformer = get_transformer(output_format)()
    output_transformer.save(output, output_format=output_format)
    return output_transformer


def neo4j_upload(inputs: List[str], input_format: str, input_compression: str, uri: str, username: str, password: str, node_filters: Optional[Tuple] = None, edge_filters: Optional[Tuple] = None) -> kgx.Transformer:
    """
    Upload a set of nodes/edges to a Neo4j database.

    Parameters
    ----------
    inputs: List[str]
        A list of files that contains nodes/edges
    input_format: str
        The input format
    input_compression: str
        The input compression type
    uri: str
        The full HTTP address for Neo4j database
    username: str
        Username for authentication
    password: str
        Password for authentication
    node_filters: Optional[Tuple]
        Node filters
    edge_filters: Optional[Tuple]
        Edge filters

    Returns
    -------
    kgx.Transformer
        The NeoTransformer

    """
    transformer = get_transformer(input_format)()
    for file in inputs:
        transformer.parse(file, input_format=input_format, compression=input_compression)
    if node_filters:
        for n in node_filters:
            transformer.set_node_filter(n[0], n[1])
    if edge_filters:
        for e in edge_filters:
            transformer.set_edge_filter(e[0], e[1])

    neo_transformer = NeoTransformer(transformer.graph, uri=uri, username=username, password=password)
    neo_transformer.save()
    return neo_transformer


def transform(inputs: List[str], input_format: str, input_compression: str, output: str, output_format: str, output_compression: str, node_filters: Optional[Tuple] = None, edge_filters: Optional[Tuple] = None) -> kgx.Transformer:
    """
    Transform a Knowledge Graph from one serialization form to another.

    Parameters
    ----------
    inputs: List[str]
        A list of files that contains nodes/edges
    input_format: str
        The input format
    input_compression: str
        The input compression type
    output: str
        The output file
    output_format: str
        The output format
    output_compression: str
        The output compression type
    node_filters: Optional[Tuple]
        Node filters
    edge_filters: Optional[Tuple]
        Edge filters

    """
    transformer = get_transformer(input_format)()
    if node_filters:
        for n in node_filters:
            transformer.set_node_filter(n[0], n[1])
    if edge_filters:
        for e in edge_filters:
            transformer.set_edge_filter(e[0], e[1])

    for file in inputs:
        transformer.parse(filename=file, input_format=input_format, compression=input_compression)

    output_transformer = get_transformer(output_format)(transformer.graph)
    output_transformer.save(output, output_format=output_format, compression=output_compression)
    return output_transformer


def merge(merge_config: str, targets: Optional[List] = None, processes: int = 1) -> networkx.MultiDiGraph:
    """
    Load nodes and edges from files and KGs, as defined in a config YAML, and merge them into a single graph.
    The merged graph can then be written to a local/remote Neo4j instance OR be serialized into a file.

    Parameters
    ----------
    merge_config: str
        Merge config YAML
    targets: Optional[List]
        A list of targets to load from the YAML
    processes: int
        Number of processes to use

    Returns
    -------
    networkx.MultiDiGraph
        The merged graph

    """
    with open(merge_config, 'r') as YML:
        cfg = yaml.load(YML, Loader=yaml.FullLoader)

    node_properties = []
    predicate_mappings = {}
    curie_map = {}
    property_types = {}
    output_directory = 'output'
    checkpoint = False

    if 'configuration' in cfg:
        if 'checkpoint' in cfg['configuration']:
            checkpoint = cfg['configuration']['checkpoint']
        if 'node_properties' in cfg['configuration']:
            node_properties = cfg['configuration']['node_properties']
        if 'predicate_mappings' in cfg['configuration']:
            predicate_mappings = cfg['configuration']['predicate_mappings']
        if 'curie_map' in cfg['configuration']:
            curie_map = cfg['configuration']['curie_map']
        if 'property_types' in cfg['configuration']:
            property_types = cfg['configuration']['property_types']
        if 'output_directory' in cfg['configuration']:
            output_directory = cfg['configuration']['output_directory']

    if not targets:
        targets = cfg['merged_graph']['targets'].keys()

    for target in targets:
        target_properties = cfg['merged_graph']['targets'][target]
        if target_properties['type'] in get_file_types():
            for f in target_properties['filename']:
                if not os.path.exists(f):
                    raise FileNotFoundError(f"Filename '{f}' for target '{target}' does not exist!")
                elif not os.path.isfile(f):
                    raise FileNotFoundError(f"Filename '{f}' for target '{target}' is not a file!")

    targets_to_parse = {}
    for key in cfg['merged_graph']['targets']:
        if key in targets:
            targets_to_parse[key] = cfg['merged_graph']['targets'][key]

    results = []
    pool = Pool(processes=processes)
    for k, v in targets_to_parse.items():
        log.info(f"Spawning process for '{k}'")
        result = pool.apply_async(parse_target, (k, v, output_directory, curie_map, node_properties, predicate_mappings, checkpoint))
        results.append(result)
    pool.close()
    pool.join()
    graphs = [r.get() for r in results]
    merged_graph = merge_all_graphs(graphs)

    if 'name' in cfg['merged_graph']:
        merged_graph.name = cfg['merged_graph']['name']
    if 'operations' in cfg['merged_graph']:
        apply_operations(cfg['merged_graph'], merged_graph)

    # write the merged graph
    if 'destination' in cfg['merged_graph']:
        for _, destination in cfg['merged_graph']['destination'].items():
            log.info(f"Writing merged graph to {_}")
            if destination['type'] == 'neo4j':
                destination_transformer = NeoTransformer(
                    source_graph=merged_graph,
                    uri=destination['uri'],
                    username=destination['username'],
                    password=destination['password']
                )
                destination_transformer.save()
            elif destination['type'] in get_file_types():
                destination_transformer = get_transformer(destination['type'])(merged_graph)
                destination_filename = f"{output_directory}/{destination['filename']}"
                if destination['type'] == 'nt' and isinstance(destination_transformer, RdfTransformer):
                    destination_transformer.set_property_types(property_types)
                compression = destination['compression'] if 'compression' in destination else None
                destination_transformer.save(
                    filename=destination_filename,
                    output_format=destination['type'],
                    compression=compression
                )
            else:
                log.error(f"type {destination['type']} not yet supported for KGX load-and-merge operation.")
    else:
        log.warning(f"No destination provided in {merge_config}. The merged graph will not be persisted.")
    return merged_graph


def parse_target(key: str, target: dict, output_directory: str, curie_map: Dict[str, str] = None, node_properties: Set[str] = None, predicate_mappings: Dict[str, str] = None, checkpoint: bool = False):
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
    checkpoint: bool
        Whether to serialize each individual target to a TSV

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
        raise TypeError("type {} not yet supported".format(target['type']))
    if checkpoint:
        log.info(f"Writing checkpoint for target '{key}'")
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
