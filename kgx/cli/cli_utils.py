import importlib
import os
import sys
from multiprocessing import Pool
from typing import List, Tuple, Any, Optional, Dict, Set

import yaml
from kgx.transformers.sssom_transformer import SssomTransformer

from kgx import PandasTransformer, NeoTransformer, Validator, RdfTransformer, NtTransformer, RsaTransformer, \
    RdfOwlTransformer, ObographJsonTransformer, JsonlTransformer, JsonTransformer, Transformer
from kgx.config import get_logger
from kgx.graph.base_graph import BaseGraph
from kgx.operations.graph_merge import merge_all_graphs
from kgx.operations.summarize_graph import summarize_graph

_transformers = {
    'tar': PandasTransformer,
    'csv': PandasTransformer,
    'tsv': PandasTransformer,
    'tsv:neo4j': PandasTransformer,
    'nt': NtTransformer,
    'ttl': RdfTransformer,
    'json': JsonTransformer,
    'jsonl': JsonlTransformer,
    'obojson': ObographJsonTransformer,
    # 'rq': kgx.SparqlTransformer,
    'owl': RdfOwlTransformer,
    'rsa': RsaTransformer,
    'sssom': SssomTransformer
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


def graph_summary(inputs: List[str], input_format: str, input_compression: Optional[str], output: Optional[str], node_facet_properties: Optional[List] = None, edge_facet_properties: Optional[List] = None) -> Dict:
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
    node_facet_properties: Optional[List]
        A list of node properties from which to generate counts per value for those properties. For example, ``['provided_by']``
    edge_facet_properties: Optional[List]
        A list of edge properties from which to generate counts per value for those properties. For example, ``['provided_by']``

    Returns
    -------
    Dict
        A dictionary with the graph stats

    """
    transformer = get_transformer(input_format)()
    for file in inputs:
        transformer.parse(file, input_format=input_format, compression=input_compression)

    stats = summarize_graph(transformer.graph, name='Graph', node_facet_properties=node_facet_properties, edge_facet_properties=edge_facet_properties)
    if output:
        WH = open(output, 'w')
        WH.write(yaml.dump(stats))
    else:
        print(yaml.dump(stats))
    return stats


def validate(inputs: List[str], input_format: str, input_compression: Optional[str], output: Optional[str]) -> List:
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

    Returns
    -------
    List
        Returns a list of errors, if any

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
    return errors


def neo4j_download(uri: str, username: str, password: str, output: str, output_format: str, output_compression: Optional[str], node_filters: Optional[Tuple] = None, edge_filters: Optional[Tuple] = None) -> Transformer:
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

    if not output_format:
        output_format = 'tsv'
    output_transformer = get_transformer(output_format)(transformer.graph)
    output_transformer.save(output, output_format=output_format)
    return output_transformer


def neo4j_upload(inputs: List[str], input_format: str, input_compression: Optional[str], uri: str, username: str, password: str, node_filters: Optional[Tuple] = None, edge_filters: Optional[Tuple] = None) -> Transformer:
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


def transform(inputs: Optional[List[str]], input_format: Optional[str] = None, input_compression: Optional[str] = None, output: Optional[str] = None, output_format: Optional[str] = None, output_compression: Optional[str] = None, node_filters: Optional[Tuple] = None, edge_filters: Optional[Tuple] = None, transform_config: str = None, source: Optional[List] = None, destination: Optional[List] = None, processes: int = 1) -> None:
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
        node_properties = []
        predicate_mappings = {}
        curie_map = {}
        property_types = {}
        checkpoint = False
        cfg = yaml.load(open(transform_config), Loader=yaml.FullLoader)
        if 'configuration' in cfg:
            if 'checkpoint' in cfg['configuration'] and cfg['configuration']['checkpoint'] is not None:
                checkpoint = cfg['configuration']['checkpoint']
            if 'node_properties' in cfg['configuration'] and cfg['configuration']['node_properties']:
                node_properties = cfg['configuration']['node_properties']
            if 'predicate_mappings' in cfg['configuration'] and cfg['configuration']['predicate_mappings']:
                predicate_mappings = cfg['configuration']['predicate_mappings']
            if 'curie_map' in cfg['configuration'] and cfg['configuration']['curie_map']:
                curie_map = cfg['configuration']['curie_map']
            if 'property_types' in cfg['configuration'] and cfg['configuration']['property_types']:
                property_types = cfg['configuration']['property_types']
            if 'output_directory' in cfg['configuration'] and cfg['configuration']['output_directory']:
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
            if source_properties['input']['format'] in get_file_types():
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
            name = v['name'] if 'name' in v else k
            result = pool.apply_async(transform_source, (name, v, output_directory, curie_map, node_properties, predicate_mappings, property_types, checkpoint, False))
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
                'filename': output
            }
        }
        name = os.path.basename(inputs[0])
        transform_source(name, source_dict, None)


def merge(merge_config: str, source: Optional[List] = None, destination: Optional[List] = None, processes: int = 1) -> BaseGraph:
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

    node_properties = []
    predicate_mappings = {}
    curie_map = {}
    property_types = {}
    output_directory = 'output'
    checkpoint = False

    if 'configuration' in cfg:
        if 'checkpoint' in cfg['configuration'] and cfg['configuration']['checkpoint'] is not None:
            checkpoint = cfg['configuration']['checkpoint']
        if 'node_properties' in cfg['configuration'] and cfg['configuration']['node_properties']:
            node_properties = cfg['configuration']['node_properties']
        if 'predicate_mappings' in cfg['configuration'] and cfg['configuration']['predicate_mappings']:
            predicate_mappings = cfg['configuration']['predicate_mappings']
        if 'curie_map' in cfg['configuration'] and cfg['configuration']['curie_map']:
            curie_map = cfg['configuration']['curie_map']
        if 'property_types' in cfg['configuration'] and cfg['configuration']['property_types']:
            property_types = cfg['configuration']['property_types']
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
        if source_properties['input']['format'] in get_file_types():
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
        name = v['name'] if 'name' in v else k
        result = pool.apply_async(parse_source, (name, v, output_directory, curie_map, node_properties, predicate_mappings, checkpoint))
        results.append(result)
    pool.close()
    pool.join()
    graphs = [r.get() for r in results]
    merged_graph = merge_all_graphs(graphs)

    if 'name' in cfg['merged_graph']:
        merged_graph.name = cfg['merged_graph']['name']
    if 'operations' in cfg['merged_graph']:
        apply_operations(cfg['merged_graph'], merged_graph)

    destination_to_write: Dict[str, Dict] = {}
    for d in destination:
        if d in cfg['merged_graph']['destination']:
            destination_to_write[d] = cfg['merged_graph']['destination'][d]
        else:
            raise KeyError(f"Cannot find destination '{d}' in YAML")

    # write the merged graph
    if destination_to_write:
        for key, destination_info in destination_to_write.items():
            log.info(f"Writing merged graph to {key}")
            if destination_info['format'] == 'neo4j':
                destination_transformer = NeoTransformer(
                    source_graph=merged_graph,
                    uri=destination_info['uri'],
                    username=destination_info['username'],
                    password=destination_info['password']
                )
                destination_transformer.save()
            elif destination_info['format'] in get_file_types():
                destination_transformer = get_transformer(destination_info['format'])(merged_graph)
                filename = destination_info['filename']
                if isinstance(filename, list):
                    filename = filename[0]
                destination_filename = f"{output_directory}/{filename}"
                if destination_info['format'] == 'nt' and isinstance(destination_transformer, RdfTransformer):
                    destination_transformer.set_predicate_mapping(predicate_mappings)
                    destination_transformer.set_property_types(property_types)
                compression = destination_info['compression'] if 'compression' in destination_info else None
                destination_transformer.save(
                    filename=destination_filename,
                    output_format=destination_info['format'],
                    compression=compression
                ) # type: ignore
            else:
                log.error(f"type {destination_info['format']} not yet supported for KGX merge operation.")
    else:
        log.warning(f"No destination provided in {merge_config}. The merged graph will not be persisted.")
    return merged_graph


def parse_source(key: str, source: dict, output_directory: str, curie_map: Dict[str, str] = None, node_properties: Set[str] = None, predicate_mappings: Dict[str, str] = None, checkpoint: bool = False):
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
    curie_map: Dict[str, str]
        Non-canonical CURIE mappings
    node_properties: Set[str]
        A set of predicates that ought to be treated as node properties (This is applicable for RDF)
    predicate_mappings: Dict[str, str]
        A mapping of predicate IRIs to property names (This is applicable for RDF)
    checkpoint: bool
        Whether to serialize each individual source to a TSV

    Returns
    -------
    kgx.graph.base_graph.BaseGraph
        Returns an instance of BaseGraph corresponding to the source

    """
    log.info(f"Processing source '{key}'")
    transformer = parse_source_input(key, source, output_directory, curie_map, node_properties, predicate_mappings, None, checkpoint)
    return transformer.graph


def transform_source(key: str, source: Dict, output_directory: Optional[str], curie_map: Dict[str, str] = None, node_properties: Set[str] = None, predicate_mappings: Dict[str, str] = None, property_types = None, checkpoint: bool = False, preserve_graph: bool = True) -> BaseGraph:
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
    curie_map: Dict[str, str]
        Non-canonical CURIE mappings
    node_properties: Set[str]
        A set of predicates that ought to be treated as node properties (This is applicable for RDF)
    predicate_mappings: Dict[str, str]
        A mapping of predicate IRIs to property names (This is applicable for RDF)
    property_types: Dict[str, str]
        The xml property type for properties that are other than ``xsd:string``.
        Relevant for RDF export.
    checkpoint: bool
        Whether to serialize each individual source to a TSV
    preserve_graph: true
        Whether or not to preserve the graph corresponding to the source

    Returns
    -------
    kgx.graph.base_graph.BaseGraph
        Returns an instance of BaseGraph corresponding to the source

    """
    log.info(f"Processing source '{key}'")
    output_format = source['output']['format']
    output_compression = source['output']['compression'] if 'compression' in source['output'] else None
    output_filename = source['output']['filename'] if 'filename' in source['output'] else key
    if isinstance(output_filename, list):
        output = output_filename[0]
    else:
        output = output_filename

    transformer = parse_source_input(key, source, output_directory, curie_map, node_properties, predicate_mappings, property_types, checkpoint)

    if output_directory and not output.startswith(output_directory):
        output = os.path.join(output_directory, output)
    if output_format == 'neo4j':
        output_transformer = NeoTransformer(
            source_graph=transformer.graph,
            uri=source['output']['uri'],
            username=source['output']['username'],
            password=source['output']['password']
        )
        output_transformer.save()
    elif output_format in get_file_types():
        output_transformer = get_transformer(output_format)(transformer.graph)
        if output_format == 'nt' and isinstance(output_transformer, RdfTransformer):
            if property_types:
                output_transformer.set_property_types(property_types)
        reify_all_edges = source['output']['reify_all_edges'] if 'reify_all_edges' in source['output'] else False
        output_transformer.save(output, output_format=output_format, compression=output_compression, reify_all_edges=reify_all_edges) # type: ignore
    else:
        raise ValueError(f"type {output_format} not yet supported for output")
    if not preserve_graph:
        output_transformer.graph.clear()
    return output_transformer.graph


def parse_source_input(key: Optional[str], source: Dict, output_directory: Optional[str], curie_map: Dict[str, str] = None, node_properties: Set[str] = None, predicate_mappings: Dict[str, str] = None, property_types = None, checkpoint: bool = False) -> Transformer:
    """
    Parse a source's input from a transform config YAML.

    Parameters
    ----------
    key: Optional[str]
        Source key
    source: Dict
        Source configuration
    output_directory: Optional[str]
        Location to write output to
    curie_map: Dict[str, str]
        Non-canonical CURIE mappings
    node_properties: Set[str]
        A set of predicates that ought to be treated as node properties (This is applicable for RDF)
    predicate_mappings: Dict[str, str]
        A mapping of predicate IRIs to property names (This is applicable for RDF)
    property_types: Dict[str, str]
        The xml property type for properties that are other than ``xsd:string``.
        Relevant for RDF export.
    checkpoint: bool
        Whether to serialize each individual source to a TSV

    Returns
    -------
    kgx.Transformer
        An instance of kgx.Transformer corresponding to the source format

    """
    if not key:
        key = os.path.basename(source['input']['filename'][0])
    source_name = source['input']['name'] if 'name' in source['input'] else key
    input_format = source['input']['format']
    input_compression = source['input']['compression'] if 'compression' in source['input'] else None
    inputs = source['input']['filename']
    filters = source['input']['filters'] if 'filters' in source['input'] and source['input']['filters'] is not None else {}
    node_filters = filters['node_filters'] if 'node_filters' in filters else {}
    edge_filters = filters['edge_filters'] if 'edge_filters' in filters else {}
    operations = source['input']['operations'] if 'operations' in source['input'] and source['input']['operations'] is not None else {}
    source_curie_map = source['curie_map'] if 'curie_map' in source and source['curie_map'] is not None else {}
    if curie_map:
        source_curie_map.update(curie_map)
    source_predicate_mappings = source['predicate_mappings'] if 'predicate_mappings' in source and source['predicate_mappings'] is not None else {}
    if predicate_mappings:
        source_predicate_mappings.update(predicate_mappings)
    source_node_properties = source['node_properties'] if 'node_properties' in source and source['node_properties'] is not None else []
    if node_properties:
        source_node_properties.extend(node_properties)

    if input_format in {'nt', 'ttl'}:
        # Parse RDF file types
        transformer = get_transformer(input_format)(curie_map=source_curie_map)
        if predicate_mappings:
            transformer.set_predicate_mapping(predicate_mappings)
        transformer.graph.name = key
        if filters:
            apply_filters(transformer, node_filters, edge_filters)
        for f in inputs:
            transformer.parse(
                filename=f,
                input_format=input_format,
                compression=input_compression,
                node_property_predicates=source_node_properties,
                provided_by=source_name
            )
        if operations:
            apply_operations(source['input'], transformer.graph)
    elif input_format in get_file_types():
        # Parse other supported file types
        transformer = get_transformer(input_format)()
        transformer.graph.name = key
        if filters:
            apply_filters(transformer, node_filters, edge_filters)
        for f in inputs:
            transformer.parse(
                filename=f,
                input_format=input_format,
                compression=input_compression,
                provided_by=source_name
            )
        if operations:
            apply_operations(source['input'], transformer.graph)
    elif input_format == 'neo4j':
        # Parse Neo4j
        transformer = NeoTransformer(
            source_graph=None,
            uri=source['uri'],
            username=source['username'],
            password=source['password']
        )
        transformer.graph.name = key
        if filters:
            apply_filters(transformer, node_filters, edge_filters)
        transformer.load(provided_by=source_name)
        if operations:
            apply_operations(source['input'], transformer.graph)
        transformer.graph.name = key
    else:
        raise TypeError(f"type {input_format} not yet supported")

    if checkpoint:
        log.info(f"Writing checkpoint for source '{key}'")
        pt = PandasTransformer(transformer.graph)
        checkpoint_output = f"{output_directory}/{key}" if output_directory else key
        pt.save(filename=checkpoint_output, output_format='tsv', compression=None)

    return transformer


def apply_filters(transformer: Transformer, node_filters: Optional[Dict], edge_filters: Optional[Dict]) -> Transformer:
    """
    Apply filters to the given transformer.

    Parameters
    ----------
    transformer: kgx.Transformer
        The transformer corresponding to the source
    node_filters: Optional[Dict]
        Node filters
    edge_filters: Optional[Dict]
        Edge filters

    Returns
    -------
    transformer: kgx.Transformer
        The transformer with filters applied

    """
    if node_filters:
        for k, v in node_filters.items():
            transformer.set_node_filter(k, set(v))
    if edge_filters:
        for k, v in edge_filters.items():
            transformer.set_edge_filter(k, set(v))
    log.info(f"with node filters: {node_filters}")
    log.info(f"with edge filters: {edge_filters}")
    return transformer


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
