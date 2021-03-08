import importlib
import os
import sys
from multiprocessing import Pool
from typing import List, Tuple, Any, Optional, Dict, Set
import yaml

from kgx.validator import Validator
from kgx.sink import Sink
from kgx.transformer import Transformer, SOURCE_MAP, SINK_MAP
from kgx.config import get_logger
from kgx.graph.base_graph import BaseGraph
from kgx.graph_operations.graph_merge import merge_all_graphs
from kgx.graph_operations import summarize_graph, knowledge_map
from kgx.utils.kgx_utils import apply_graph_operations


summary_report_types = {
    'kgx-map': summarize_graph.summarize_graph,
    'knowledge-map': knowledge_map.summarize_graph,
}

log = get_logger()


def get_input_file_types() -> Tuple:
    """
    Get all input file formats supported by KGX.

    Returns
    -------
    Tuple
        A tuple of supported file formats

    """
    return tuple(SOURCE_MAP.keys())


def get_output_file_types() -> Tuple:
    """
    Get all output file formats supported by KGX.

    Returns
    -------
    Tuple
        A tuple of supported file formats

    """
    return tuple(SINK_MAP.keys())


def graph_summary(
    inputs: List[str],
    input_format: str,
    input_compression: Optional[str],
    output: Optional[str],
    report_type: str,
    stream: bool = False,
    node_facet_properties: Optional[List] = None,
    edge_facet_properties: Optional[List] = None,
) -> Dict:
    """
    Loads and summarizes a knowledge graph from a set of input files.

    Parameters
    ----------
    inputs: List[str]
        Input file
    input_format: str
        Input file format
    input_compression: Optional[str]
        The input compression type
    output: Optional[str]
        Where to write the output (stdout, by default)
    report_type: str
        The summary report type
    stream: bool
        Whether to parse input as a stream
    node_facet_properties: Optional[List]
        A list of node properties from which to generate counts per value for those properties. For example, ``['provided_by']``
    edge_facet_properties: Optional[List]
        A list of edge properties from which to generate counts per value for those properties. For example, ``['provided_by']``

    Returns
    -------
    Dict
        A dictionary with the graph stats

    """
    if stream:
        log.info("stream processing not supported. Setting stream to 'False'")
        stream = False
    transformer = Transformer(stream=stream)
    transformer.transform(
        {'filename': inputs, 'format': input_format, 'compression': input_compression}
    )
    if report_type in summary_report_types:
        stats = summary_report_types[report_type](
            graph=transformer.store.graph,
            name='Graph',
            node_facet_properties=node_facet_properties,
            edge_facet_properties=edge_facet_properties,
        )
    else:
        raise ValueError(f"report_type must be one of {summary_report_types.keys()}")
    if output:
        WH = open(output, 'w')
        WH.write(yaml.dump(stats))
    else:
        print(yaml.dump(stats))
    return stats


def validate(
    inputs: List[str],
    input_format: str,
    input_compression: Optional[str],
    output: Optional[str],
    stream: bool,
) -> List:
    """
    Run KGX validator on an input file to check for Biolink Model compliance.

    Parameters
    ----------
    inputs: List[str]
        Input files
    input_format: str
        The input format
    input_compression: Optional[str]
        The input compression type
    output: Optional[str]
        Path to output file (stdout, by default)
    stream: bool
        Whether to parse input as a stream
    Returns
    -------
    List
        Returns a list of errors, if any

    """
    if stream:
        log.info("stream processing not supported. Setting stream to 'False'")
        stream = False
    transformer = Transformer(stream=stream)
    transformer.transform(
        {'filename': inputs, 'format': input_format, 'compression': input_compression}
    )

    validator = Validator()
    errors = validator.validate(transformer.store.graph)
    if output:
        validator.write_report(errors, open(output, 'w'))
    else:
        validator.write_report(errors, sys.stdout)
    return errors


def neo4j_download(
    uri: str,
    username: str,
    password: str,
    output: str,
    output_format: str,
    output_compression: Optional[str],
    stream: bool,
    node_filters: Optional[Tuple] = None,
    edge_filters: Optional[Tuple] = None,
) -> Transformer:
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
    output_format: Optional[str]
        The output type (``tsv``, by default)
    output_compression: Optional[str]
        The output compression type
    stream: bool
        Whether to parse input as a stream
    node_filters: Optional[Tuple]
        Node filters
    edge_filters: Optional[Tuple]
        Edge filters

    Returns
    -------
    kgx.Transformer
        The NeoTransformer

    """
    transformer = Transformer(stream=stream)
    transformer.transform(
        {
            'uri': uri,
            'username': username,
            'password': password,
            'format': 'neo4j',
            'node_filters': node_filters,
            'edge_filters': edge_filters,
        }
    )

    if not output_format:
        output_format = 'tsv'
    transformer.save(
        {'filename': output, 'format': output_format, 'compression': output_compression}
    )
    return transformer


def neo4j_upload(
    inputs: List[str],
    input_format: str,
    input_compression: Optional[str],
    uri: str,
    username: str,
    password: str,
    stream: bool,
    node_filters: Optional[Tuple] = None,
    edge_filters: Optional[Tuple] = None,
) -> Transformer:
    """
    Upload a set of nodes/edges to a Neo4j database.

    Parameters
    ----------
    inputs: List[str]
        A list of files that contains nodes/edges
    input_format: str
        The input format
    input_compression: Optional[str]
        The input compression type
    uri: str
        The full HTTP address for Neo4j database
    username: str
        Username for authentication
    password: str
        Password for authentication
    stream: bool
        Whether to parse input as a stream
    node_filters: Optional[Tuple]
        Node filters
    edge_filters: Optional[Tuple]
        Edge filters

    Returns
    -------
    kgx.Transformer
        The NeoTransformer

    """
    transformer = Transformer(stream=stream)
    transformer.transform(
        {
            'filename': inputs,
            'format': input_format,
            'compression': input_compression,
            'node_filters': node_filters,
            'edge_filters': edge_filters,
        }
    )
    transformer.save({'uri': uri, 'username': username, 'password': password, 'format': 'neo4j'})
    return transformer


def transform(
    inputs: Optional[List[str]],
    input_format: Optional[str] = None,
    input_compression: Optional[str] = None,
    output: Optional[str] = None,
    output_format: Optional[str] = None,
    output_compression: Optional[str] = None,
    stream: bool = False,
    node_filters: Optional[Tuple] = None,
    edge_filters: Optional[Tuple] = None,
    transform_config: str = None,
    source: Optional[List] = None,
    destination: Optional[List] = None,
    processes: int = 1,
) -> None:
    """
    Transform a Knowledge Graph from one serialization form to another.

    Parameters
    ----------
    inputs: Optional[List[str]]
        A list of files that contains nodes/edges
    input_format: Optional[str]
        The input format
    input_compression: Optional[str]
        The input compression type
    output: Optional[str]
        The output file
    output_format: Optional[str]
        The output format
    output_compression: Optional[str]
        The output compression type
    stream: bool
        Whether to parse input as a stream
    node_filters: Optional[Tuple]
        Node filters
    edge_filters: Optional[Tuple]
        Edge filters
    transform_config: Optional[str]
        The transform config YAML
    source: Optional[List]
        A list of source to load from the YAML
    destination: Optional[List]
        A list of destination to write to, as defined in the YAML
    processes: int
        Number of processes to use

    """
    if transform_config and inputs:
        raise ValueError("Can accept either --transform-config OR inputs, not both")

    output_directory = 'output'

    if transform_config:
        cfg = yaml.load(open(transform_config), Loader=yaml.FullLoader)
        top_level_args = {}
        if 'configuration' in cfg:
            top_level_args = prepare_top_level_args(cfg['configuration'])
            if (
                'output_directory' in cfg['configuration']
                and cfg['configuration']['output_directory']
            ):
                output_directory = cfg['configuration']['output_directory']
                if not output_directory.startswith(os.path.sep):
                    # relative path
                    output_directory = f"{os.path.abspath(os.path.dirname(transform_config))}{os.path.sep}{output_directory}"

        if not os.path.exists(output_directory):
            os.mkdir(output_directory)

        if not source:
            source = cfg['transform']['source'].keys()
        for s in source:
            source_properties = cfg['transform']['source'][s]
            if source_properties['input']['format'] in get_input_file_types():
                for f in source_properties['input']['filename']:
                    if not os.path.exists(f):
                        raise FileNotFoundError(f"Filename '{f}' for source '{s}' does not exist!")
                    elif not os.path.isfile(f):
                        raise FileNotFoundError(f"Filename '{f}' for source '{s}' is not a file!")

        source_to_parse = {}
        for key, val in cfg['transform']['source'].items():
            if key in source:
                source_to_parse[key] = val

        results = []
        pool = Pool(processes=processes)
        for k, v in source_to_parse.items():
            log.info(f"Spawning process for '{k}'")
            result = pool.apply_async(
                transform_source,
                (
                    k,
                    v,
                    output_directory,
                    top_level_args['prefix_map'],
                    top_level_args['node_property_predicates'],
                    top_level_args['predicate_mappings'],
                    top_level_args['reverse_prefix_map'],
                    top_level_args['reverse_predicate_mappings'],
                    top_level_args['property_types'],
                    top_level_args['checkpoint'],
                    False,
                    stream,
                ),
            )
            results.append(result)
        pool.close()
        pool.join()
        graphs = [r.get() for r in results]
    else:
        source_dict: Dict = {
            'input': {
                'format': input_format,
                'compression': input_compression,
                'filename': inputs,
            },
            'output': {
                'format': output_format,
                'compression': output_compression,
                'filename': output,
            },
        }
        name = os.path.basename(inputs[0])
        transform_source(key=name, source=source_dict, output_directory=None, stream=stream)


def merge(
    merge_config: str,
    source: Optional[List] = None,
    destination: Optional[List] = None,
    processes: int = 1,
) -> BaseGraph:
    """
    Load nodes and edges from files and KGs, as defined in a config YAML, and merge them into a single graph.
    The merged graph can then be written to a local/remote Neo4j instance OR be serialized into a file.

    Parameters
    ----------
    merge_config: str
        Merge config YAML
    source: Optional[List]
        A list of source to load from the YAML
    destination: Optional[List]
        A list of destination to write to, as defined in the YAML
    processes: int
        Number of processes to use

    Returns
    -------
    kgx.graph.base_graph.BaseGraph
        The merged graph

    """
    with open(merge_config, 'r') as YML:
        cfg = yaml.load(YML, Loader=yaml.FullLoader)

    output_directory = 'output'

    top_level_args = {}
    if 'configuration' in cfg:
        top_level_args = prepare_top_level_args(cfg['configuration'])
        if 'output_directory' in cfg['configuration'] and cfg['configuration']['output_directory']:
            output_directory = cfg['configuration']['output_directory']
            if not output_directory.startswith(os.path.sep):
                # relative path
                output_directory = f"{os.path.abspath(os.path.dirname(merge_config))}{os.path.sep}{output_directory}"

    if not os.path.exists(output_directory):
        os.mkdir(output_directory)

    if not source:
        source = cfg['merged_graph']['source'].keys()

    if not destination:
        destination = cfg['merged_graph']['destination'].keys()

    for s in source:
        source_properties = cfg['merged_graph']['source'][s]
        if source_properties['input']['format'] in get_input_file_types():
            for f in source_properties['input']['filename']:
                if not os.path.exists(f):
                    raise FileNotFoundError(f"Filename '{f}' for source '{s}' does not exist!")
                elif not os.path.isfile(f):
                    raise FileNotFoundError(f"Filename '{f}' for source '{s}' is not a file!")

    sources_to_parse = {}
    for key in cfg['merged_graph']['source']:
        if key in source:
            sources_to_parse[key] = cfg['merged_graph']['source'][key]

    results = []
    pool = Pool(processes=processes)
    for k, v in sources_to_parse.items():
        log.info(f"Spawning process for '{k}'")
        result = pool.apply_async(
            parse_source,
            (
                k,
                v,
                output_directory,
                top_level_args['prefix_map'],
                top_level_args['node_property_predicates'],
                top_level_args['predicate_mappings'],
                top_level_args['checkpoint'],
            ),
        )
        results.append(result)
    pool.close()
    pool.join()
    stores = [r.get() for r in results]
    merged_graph = merge_all_graphs([x.graph for x in stores])
    log.info(
        f"Merged graph has {merged_graph.number_of_nodes()} nodes and {merged_graph.number_of_edges()} edges"
    )
    if 'name' in cfg['merged_graph']:
        merged_graph.name = cfg['merged_graph']['name']
    if 'operations' in cfg['merged_graph']:
        apply_graph_operations(merged_graph, cfg['merged_graph']['operations'])

    destination_to_write: Dict[str, Dict] = {}
    for d in destination:
        if d in cfg['merged_graph']['destination']:
            destination_to_write[d] = cfg['merged_graph']['destination'][d]
        else:
            raise KeyError(f"Cannot find destination '{d}' in YAML")

    # write the merged graph
    node_properties = set()
    edge_properties = set()
    for s in stores:
        node_properties.update(s.node_properties)
        edge_properties.update(s.edge_properties)

    input_args = {'graph': merged_graph, 'format': 'graph'}
    if destination_to_write:
        for key, destination_info in destination_to_write.items():
            log.info(f"Writing merged graph to {key}")
            output_args = {
                'format': destination_info['format'],
                'reverse_prefix_map': top_level_args['reverse_prefix_map'],
                'reverse_predicate_mappings': top_level_args['reverse_predicate_mappings'],
            }
            if 'reverse_prefix_map' in destination_info:
                output_args['reverse_prefix_map'].update(destination_info['reverse_prefix_map'])
            if 'reverse_predicate_mappings' in destination_info:
                output_args['reverse_predicate_mappings'].update(
                    destination_info['reverse_predicate_mappings']
                )
            if destination_info['format'] == 'neo4j':
                output_args['uri'] = destination_info['uri']
                output_args['username'] = destination_info['username']
                output_args['password'] = destination_info['password']
            elif destination_info['format'] in get_input_file_types():
                filename = destination_info['filename']
                if isinstance(filename, list):
                    filename = filename[0]
                destination_filename = f"{output_directory}/{filename}"
                output_args['filename'] = destination_filename
                output_args['compression'] = (
                    destination_info['compression'] if 'compression' in destination_info else None
                )
                if destination_info['format'] == 'nt':
                    output_args['property_types'] = top_level_args['property_types']
                    if 'property_types' in top_level_args:
                        output_args['property_types'].update(destination_info['property_types'])
                if destination_info['format'] in {'csv', 'tsv'}:
                    output_args['node_properties'] = node_properties
                    output_args['edge_properties'] = edge_properties
            else:
                raise TypeError(
                    f"type {destination_info['format']} not yet supported for KGX merge operation."
                )
            transformer = Transformer()
            transformer.transform(input_args, output_args)
    else:
        log.warning(
            f"No destination provided in {merge_config}. The merged graph will not be persisted."
        )
    return merged_graph


def parse_source(
    key: str,
    source: dict,
    output_directory: str,
    prefix_map: Dict[str, str] = None,
    node_property_predicates: Set[str] = None,
    predicate_mappings: Dict[str, str] = None,
    checkpoint: bool = False,
) -> Sink:
    """
    Parse a source from a merge config YAML.

    Parameters
    ----------
    key: str
        Source key
    source: Dict
        Source configuration
    output_directory: str
        Location to write output to
    prefix_map: Dict[str, str]
        Non-canonical CURIE mappings
    node_property_predicates: Set[str]
        A set of predicates that ought to be treated as node properties (This is applicable for RDF)
    predicate_mappings: Dict[str, str]
        A mapping of predicate IRIs to property names (This is applicable for RDF)
    checkpoint: bool
        Whether to serialize each individual source to a TSV

    Returns
    -------
    kgx.sink.sink.Sink
        Returns an instance of Sink

    """
    log.info(f"Processing source '{key}'")
    if not key:
        key = os.path.basename(source['input']['filename'][0])
    input_args = prepare_input_args(
        key, source, output_directory, prefix_map, node_property_predicates, predicate_mappings
    )
    transformer = Transformer()
    transformer.transform(input_args)
    transformer.store.graph.name = key
    if checkpoint:
        log.info(f"Writing checkpoint for source '{key}'")
        checkpoint_output = f"{output_directory}/{key}" if output_directory else key
        transformer.save({'filename': checkpoint_output, 'format': 'tsv'})
    return transformer.store


def transform_source(
    key: str,
    source: Dict,
    output_directory: Optional[str],
    prefix_map: Dict[str, str] = None,
    node_property_predicates: Set[str] = None,
    predicate_mappings: Dict[str, str] = None,
    reverse_prefix_map: Dict = None,
    reverse_predicate_mappings: Dict = None,
    property_types: Dict = None,
    checkpoint: bool = False,
    preserve_graph: bool = True,
    stream: bool = False,
) -> Sink:
    """
    Transform a source from a transform config YAML.

    Parameters
    ----------
    key: str
        Source key
    source: Dict
        Source configuration
    output_directory: Optional[str]
        Location to write output to
    prefix_map: Dict[str, str]
        Non-canonical CURIE mappings
    node_property_predicates: Set[str]
        A set of predicates that ought to be treated as node properties (This is applicable for RDF)
    predicate_mappings: Dict[str, str]
        A mapping of predicate IRIs to property names (This is applicable for RDF)
    reverse_prefix_map: Dict[str, str]
        Non-canonical CURIE mappings for export
    reverse_predicate_mappings: Dict[str, str]
        A mapping of property names to predicate IRIs (This is applicable for RDF)
    property_types: Dict[str, str]
        The xml property type for properties that are other than ``xsd:string``.
        Relevant for RDF export.
    checkpoint: bool
        Whether to serialize each individual source to a TSV
    preserve_graph: true
        Whether or not to preserve the graph corresponding to the source
    stream: bool
        Whether to parse input as a stream

    Returns
    -------
    kgx.sink.sink.Sink
        Returns an instance of Sink

    """
    log.info(f"Processing source '{key}'")
    input_args = prepare_input_args(
        key, source, output_directory, prefix_map, node_property_predicates, predicate_mappings
    )
    output_args = prepare_output_args(
        key,
        source,
        output_directory,
        reverse_prefix_map,
        reverse_predicate_mappings,
        property_types,
    )
    transformer = Transformer(stream=stream)
    transformer.transform(input_args, output_args)
    if not preserve_graph:
        transformer.store.graph.clear()
    return transformer.store


def prepare_input_args(
    key: str,
    source: Dict,
    output_directory: Optional[str],
    prefix_map: Dict[str, str] = None,
    node_property_predicates: Set[str] = None,
    predicate_mappings: Dict[str, str] = None,
) -> Dict:
    """
    Prepare input arguments for Transformer.

    Parameters
    ----------
    key: str
        Source key
    source: Dict
        Source configuration
    output_directory: str
        Location to write output to
    prefix_map: Dict[str, str]
        Non-canonical CURIE mappings
    node_property_predicates: Set[str]
        A set of predicates that ought to be treated as node properties (This is applicable for RDF)
    predicate_mappings: Dict[str, str]
        A mapping of predicate IRIs to property names (This is applicable for RDF)

    Returns
    -------
    Dict
        Input arguments as dictionary

    """
    if not key:
        key = os.path.basename(source['input']['filename'][0])
    source_name = source['name'] if 'name' in source else key
    input_format = source['input']['format']
    input_compression = source['input']['compression'] if 'compression' in source['input'] else None
    inputs = source['input']['filename']
    filters = (
        source['input']['filters']
        if 'filters' in source['input'] and source['input']['filters'] is not None
        else {}
    )
    node_filters = filters['node_filters'] if 'node_filters' in filters else {}
    edge_filters = filters['edge_filters'] if 'edge_filters' in filters else {}
    source_prefix_map = prefix_map.copy() if prefix_map else {}
    source_prefix_map.update(
        source['prefix_map'] if 'prefix_map' in source and source['prefix_map'] else {}
    )
    source_predicate_mappings = predicate_mappings.copy() if predicate_mappings else {}
    source_predicate_mappings.update(
        source['predicate_mappings']
        if 'predicate_mappings' in source and source['predicate_mappings'] is not None
        else {}
    )
    source_node_property_predicates = (
        node_property_predicates if node_property_predicates else set()
    )
    source_node_property_predicates.update(
        source['node_property_predicates']
        if 'node_property_predicates' in source and source['node_property_predicates'] is not None
        else set()
    )

    if input_format in {'nt', 'ttl'}:
        input_args = {
            'filename': inputs,
            'format': input_format,
            'compression': input_compression,
            'provided_by': source_name,
            'node_filters': node_filters,
            'edge_filters': edge_filters,
            'prefix_map': source_prefix_map,
            'predicate_mappings': source_predicate_mappings,
            'node_property_predicates': source_node_property_predicates,
        }
    elif input_format in get_input_file_types():
        input_args = {
            'filename': inputs,
            'format': input_format,
            'compression': input_compression,
            'provided_by': source_name,
            'node_filters': node_filters,
            'edge_filters': edge_filters,
            'prefix_map': source_prefix_map,
        }
    elif input_format == 'neo4j':
        input_args = {
            'uri': source['uri'],
            'username': source['username'],
            'password': source['password'],
            'format': input_format,
            'provided_by': source_name,
            'node_filters': node_filters,
            'edge_filters': edge_filters,
            'prefix_map': prefix_map,
        }
    else:
        raise TypeError(f"Type {input_format} not yet supported")

    input_args['operations'] = source['input'].get('operations', [])
    for o in input_args['operations']:
        args = o['args']
        if 'filename' in args:
            filename = args['filename']
            if not filename.startswith(output_directory):
                o['args'] = os.path.join(output_directory, filename)
    return input_args


def prepare_output_args(
    key: str,
    source: Dict,
    output_directory: Optional[str],
    reverse_prefix_map: Dict = None,
    reverse_predicate_mappings: Dict = None,
    property_types: Dict = None,
) -> Dict:
    """
    Prepare output arguments for Transformer.

    Parameters
    ----------
    key: str
        Source key
    source: Dict
        Source configuration
    output_directory: str
        Location to write output to
    reverse_prefix_map: Dict[str, str]
        Non-canonical CURIE mappings for export
    reverse_predicate_mappings: Dict[str, str]
        A mapping of property names to predicate IRIs (This is applicable for RDF)
    property_types: Dict[str, str]
        The xml property type for properties that are other than ``xsd:string``.
        Relevant for RDF export.

    Returns
    -------
    Dict
        Output arguments as dictionary

    """
    output_format = source['output']['format']
    output_compression = (
        source['output']['compression'] if 'compression' in source['output'] else None
    )
    output_filename = source['output']['filename'] if 'filename' in source['output'] else key
    source_reverse_prefix_map = reverse_prefix_map.copy() if reverse_prefix_map else {}
    source_reverse_prefix_map.update(
        source['reverse_prefix_map']
        if 'reverse_prefix_map' in source and source['reverse_prefix_map']
        else {}
    )
    source_reverse_predicate_mappings = (
        reverse_predicate_mappings.copy() if reverse_predicate_mappings else {}
    )
    source_reverse_predicate_mappings.update(
        source['reverse_predicate_mappings']
        if 'reverse_predicate_mappings' in source
        and source['reverse_predicate_mappings'] is not None
        else {}
    )
    source_property_types = property_types.copy() if property_types else {}
    source_property_types.update(source['property_types']) if 'property_types' in source and source[
        'property_types'
    ] is not None else {}

    if isinstance(output_filename, list):
        output = output_filename[0]
    else:
        output = output_filename
    if output_directory and not output.startswith(output_directory):
        output = os.path.join(output_directory, output)
    output_args = {'format': output_format}
    if output_format == 'neo4j':
        output_args['uri'] = source['output']['uri']
        output_args['username'] = source['output']['username']
        output_args['password'] = source['output']['password']
    elif output_format in get_input_file_types():
        output_args['filename'] = output
        output_args['compression'] = output_compression
        if output_format == 'nt':
            output_args['reify_all_edges'] = (
                source['output']['reify_all_edges']
                if 'reify_all_edges' in source['output']
                else False
            )
            output_args['reverse_prefix_map'] = source_reverse_prefix_map
            output_args['reverse_predicate_mappings'] = source_reverse_predicate_mappings
            output_args['property_types'] = source_property_types
    else:
        raise ValueError(f"type {output_format} not yet supported for output")
    return output_args


def apply_operations(source: dict, graph: BaseGraph) -> BaseGraph:
    """
    Apply operations as defined in the YAML.

    Parameters
    ----------
    source: dict
        The source from the YAML
    graph: kgx.graph.base_graph.BaseGraph
        The graph corresponding to the source

    Returns
    -------
    kgx.graph.base_graph.BaseGraph
        The graph corresponding to the source

    """
    operations = source['operations']
    for operation in operations:
        op_name = operation['name']
        op_args = operation['args']
        module_name = '.'.join(op_name.split('.')[0:-1])
        function_name = op_name.split('.')[-1]
        f = getattr(importlib.import_module(module_name), function_name)
        log.info(f"Applying operation {op_name} with args: {op_args}")
        f(graph, **op_args)
    return graph


def prepare_top_level_args(d: Dict) -> Dict:
    """
    Parse top-level configuration.

    Parameters
    ----------
    d: Dict
        The configuration section from the transform/merge YAML

    Returns
    -------
    Dict
        A parsed dictionary with parameters from configuration

    """
    args = {}
    if 'checkpoint' in d and d['checkpoint'] is not None:
        args['checkpoint'] = d['checkpoint']
    else:
        args['checkpoint'] = False
    if 'node_property_predicates' in d and d['node_property_predicates']:
        args['node_property_predicates'] = set(d['node_property_predicates'])
    else:
        args['node_property_predicates'] = set()
    if 'predicate_mappings' in d and d['predicate_mappings']:
        args['predicate_mappings'] = d['predicate_mappings']
    else:
        args['predicate_mappings'] = {}
    if 'prefix_map' in d and d['prefix_map']:
        args['prefix_map'] = d['prefix_map']
    else:
        args['prefix_map'] = {}
    if 'reverse_prefix_map' in d and d['reverse_prefix_map'] is not None:
        args['reverse_prefix_map'] = d['reverse_prefix_map']
    else:
        args['reverse_prefix_map'] = {}
    if 'reverse_predicate_mappings' in d and d['reverse_predicate_mappings'] is not None:
        args['reverse_predicate_mappings'] = d['reverse_predicate_mappings']
    else:
        args['reverse_predicate_mappings'] = {}
    if 'property_types' in d and d['property_types']:
        args['property_types'] = d['property_types']
    else:
        args['property_types'] = {}
    return args
