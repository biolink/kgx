from kgx import NeoTransformer
from kgx.operations.summarize_graph import summarize_graph

import kgx
import os, sys, click, logging, yaml
from typing import List, Tuple

from kgx.config import get_logger, get_config
from kgx.operations.graph_merge import merge_all_graphs
from kgx.validator import Validator
from kgx.cli.cli_utils import get_file_types, get_transformer, apply_operations, apply_filters

log = get_logger()
config = get_config()


def error(msg):
    log.error(msg)
    quit()


@click.group()
@click.version_option(version=kgx.__version__, prog_name=kgx.__name__)
def cli():
    """
    Knowledge Graph Exchange CLI entrypoint.
    \f

    """
    pass


@cli.command('graph-summary')
@click.argument('inputs', required=True, type=click.Path(exists=True), nargs=-1)
@click.option('--input-format', required=True, type=click.Choice(get_file_types()), help='The input format')
@click.option('--input-compression', required=False, help='The input compression type')
@click.option('--output', required=True, type=click.Path(exists=False))
def graph_summary(inputs: List[str], input_format: str, input_compression: str, output: str):
    """
    Loads and summarizes a knowledge graph from a set of input files.
    \f

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


@cli.command()
@click.argument('inputs',  required=True, type=click.Path(exists=True), nargs=-1)
@click.option('--input-format', required=True, help=f'The input format. Can be one of {get_file_types()}')
@click.option('--input-compression', required=False, help='The input compression type')
@click.option('--output', required=False, type=click.Path(exists=False), help='File to write validation reports to')
def validate(inputs: List[str], input_format: str, input_compression: str, output: str):
    """
    Run KGX validator on an input file to check for BioLink Model compliance.
    \f

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

    """
    transformer = get_transformer(input_format)()
    for file in inputs:
        transformer.parse(file, input_format=input_format, input_compression=input_compression)

    validator = Validator()
    errors = validator.validate(transformer.graph)
    if output:
        validator.write_report(errors, open(output, 'w'))
    else:
        validator.write_report(errors, sys.stdout)


@cli.command(name='neo4j-download')
@click.option('--uri', required=True, type=str, help='Neo4j URI to download from. For example, https://localhost:7474')
@click.option('--username', required=True, type=str, help='Neo4j username')
@click.option('--password', required=True, type=str, help='Neo4j password')
@click.option('--output', required=True, type=click.Path(exists=False), help='Output')
@click.option('--output-format', required=True, help=f'The output format. Can be one of {get_file_types()}')
@click.option('--output-compression', required=False, help='The output compression type')
@click.option('--node-filters', required=False, type=click.Tuple([str, str]), multiple=True, help=f'Filters for filtering nodes from the input graph')
@click.option('--edge-filters', required=False, type=click.Tuple([str, str]), multiple=True, help=f'Filters for filtering edges from the input graph')
def neo4j_download(uri: str, username: str, password: str, output: str, output_format: str, output_compression: str, node_filters: Tuple, edge_filters: Tuple):
    """
    Download nodes and edges from Neo4j database.
    \f

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
    node_filters: Tuple[str, str]
        Node filters
    edge_filters: Tuple[str, str]
        Edge filters

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


@cli.command(name='neo4j-upload')
@click.argument('inputs',  required=True, type=click.Path(exists=True), nargs=-1)
@click.option('--input-format', required=True, help=f'The input format. Can be one of {get_file_types()}')
@click.option('--input-compression', required=False, help='The input compression type')
@click.option('--uri', required=True, type=str, help='Neo4j URI to upload to. For example, https://localhost:7474')
@click.option('--username', required=True, type=str, help='Neo4j username')
@click.option('--password', required=True, type=str, help='Neo4j password')
@click.option('--node-filters', required=False, type=click.Tuple([str, str]), multiple=True, help=f'Filters for filtering nodes from the input graph')
@click.option('--edge-filters', required=False, type=click.Tuple([str, str]), multiple=True, help=f'Filters for filtering edges from the input graph')
def neo4j_upload(inputs: List[str], input_format: str, input_compression: str, uri: str, username: str, password: str, node_filters: Tuple[str, str], edge_filters: Tuple[str, str]):
    """
    Upload a set of nodes/edges to a Neo4j database.
    \f

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
    node_filters: Tuple[str, str]
        Node filters
    edge_filters: Tuple[str, str]
        Edge filters

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


@cli.command()
@click.argument('inputs',  required=True, type=click.Path(exists=True), nargs=-1)
@click.option('--input-format', required=True, help=f'The input format. Can be one of {get_file_types()}')
@click.option('--input-compression', required=False, help='The input compression type')
@click.option('--output', required=True, type=click.Path(exists=False), help='Output')
@click.option('--output-format', required=True, help=f'The output format. Can be one of {get_file_types()}')
@click.option('--output-compression', required=False, help='The output compression type')
@click.option('--node-filters', required=False, type=click.Tuple([str, str]), multiple=True, help=f'Filters for filtering nodes from the input graph')
@click.option('--edge-filters', required=False, type=click.Tuple([str, str]), multiple=True, help=f'Filters for filtering edges from the input graph')
def transform(inputs: List[str], input_format: str, input_compression: str, output: str, output_format: str, output_compression: str, node_filters: Tuple, edge_filters: Tuple):
    """
    Transform a Knowledge Graph from one serialization form to another.
    \f

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
    node_filters: Tuple[str, str]
        Node filters
    edge_filters: Tuple[str, str]
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


@cli.command(name='merge')
@click.argument('merge-config', required=True, type=click.Path(exists=True))
def merge(merge_config: str):
    """
    Load nodes and edges from files and KGs, as defined in a config YAML, and merge them into a single graph.
    The merged graph can then be written to a local/remote Neo4j instance OR be serialized into a file.
    \f

    .. note::
        Everything here is driven by the ``merge-config`` YAML.

    Parameters
    ----------
    merge_config: str
        Merge config YAML

    """
    with open(merge_config, 'r') as YML:
        cfg = yaml.load(YML, Loader=yaml.FullLoader)

    node_properties = []
    predicate_mappings = {}
    curie_map = {}
    if 'configuration' in cfg:
        if 'node_properties' in cfg['configuration']:
            node_properties = cfg['configuration']['node_properties']
        if 'predicate_mappings' in cfg['configuration']:
            predicate_mappings = cfg['configuration']['predicate_mappings']
        if 'curie_map' in cfg['configuration']:
            curie_map = cfg['configuration']['curie_map']

    for key in cfg['target']:
        target = cfg['target'][key]
        if target['type'] in get_file_types():
            for f in target['filename']:
                if not os.path.exists(f):
                    raise FileNotFoundError(f"Filename '{f}' for target '{key}' does not exist!")
                elif not os.path.isfile(f):
                    raise FileNotFoundError(f"Filename '{f}' for target '{key}' is not a file!")

    transformers = []
    for key in cfg['target']:
        log.info(f"Processing target '{key}'")
        target = cfg['target'][key]
        target_curie_map = target['curie_map'] if 'curie_map' in target else {}
        target_curie_map.update(curie_map)
        target_predicate_mappings = target['predicate_mappings'] if 'predicate_mappings' in target else {}
        target_predicate_mappings.update(predicate_mappings)
        target_node_properties = target['node_properties'] if 'node_properties' in target else []
        target_node_properties.extend(node_properties)

        input_format = target['type']
        compression = target['compression'] if 'compression' in target else None

        logging.info("Loading {}".format(key))
        if target['type'] in {'nt', 'ttl'}:
            # Parse RDF file types
            transformer = get_transformer(target['type'])(curie_map=target_curie_map)
            transformer.set_predicate_mapping(predicate_mappings)
            transformer.graph.name = key
            if 'filters' in target:
                apply_filters(target, transformer)
            for f in target['filename']:
                transformer.parse(f, input_format=input_format, compression=compression, node_property_predicates=target_node_properties)
            if 'operations' in target:
                apply_operations(target, transformer)
            transformers.append(transformer)
        elif target['type'] in get_file_types():
            # Parse other supported file types
            transformer = get_transformer(target['type'])()
            transformer.graph.name = key
            if 'filters' in target:
                apply_filters(target, transformer)
            for f in target['filename']:
                transformer.parse(f, input_format=input_format, compression=compression)
            if 'operations' in target:
                apply_operations(target, transformer)
            transformers.append(transformer)
        elif target['type'] == 'neo4j':
            # Parse Neo4j
            transformer = NeoTransformer(None, target['uri'], target['username'],  target['password'])
            transformer.graph.name = key
            if 'filters' in target:
                apply_filters(target, transformer)
            transformer.load()
            if 'operations' in target:
                apply_operations(target, transformer)
            transformers.append(transformer)
            transformer.graph.name = key
        else:
            logging.error("type {} not yet supported".format(target['type']))
    merged_graph = merge_all_graphs([x.graph for x in transformers])

    # write the merged graph
    if 'destination' in cfg:
        for _, destination in cfg['destination'].items():
            if destination['type'] == 'neo4j':
                destination_transformer = NeoTransformer(
                    merged_graph,
                    uri=destination['uri'],
                    username=destination['username'],
                    password=destination['password']
                )
                destination_transformer.save()
            elif destination['type'] in get_file_types():
                destination_transformer = get_transformer(destination['type'])(merged_graph)
                compression = destination['compression'] if 'compression' in destination else None
                destination_transformer.save(destination['filename'], output_format=destination['type'], compression=compression)
            else:
                log.error(f"type {destination['type']} not yet supported for KGX load-and-merge operation.")
    else:
        log.warning(f"No destination provided in {merge_config}. The merged graph will not be persisted.")
