import kgx
import click
from typing import List, Tuple, Optional, Set

from kgx.config import get_logger, get_config
from kgx.cli.cli_utils import (
    get_input_file_types,
    parse_source,
    apply_operations,
    graph_summary,
    validate,
    neo4j_download,
    neo4j_upload,
    transform,
    merge,
    summary_report_types,
)

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
@click.option(
    '--input-format',
    required=True,
    help=f'The input format. Can be one of {get_input_file_types()}',
)
@click.option('--input-compression', required=False, help='The input compression type')
@click.option('--output', required=True, type=click.Path(exists=False))
@click.option(
    '--report-type',
    required=False,
    type=str,
    help=f'The summary report type. Can be one of {summary_report_types.keys()}',
    default='kgx-map',
)
@click.option('--stream', is_flag=True, help='Parse input as a stream')
@click.option(
    '--node-facet-properties',
    required=False,
    multiple=True,
    help='A list of node properties from which to generate counts per value for those properties',
)
@click.option(
    '--edge-facet-properties',
    required=False,
    multiple=True,
    help='A list of edge properties from which to generate counts per value for those properties',
)
def graph_summary_wrapper(
    inputs: List[str],
    input_format: str,
    input_compression: str,
    output: str,
    report_type: str,
    stream: bool,
    node_facet_properties: Optional[Set],
    edge_facet_properties: Optional[Set],
):
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
    report_type: str
        The summary report type
    stream: bool
        Whether to parse input as a stream
    node_facet_properties: Optional[Set]
        A list of node properties from which to generate counts per value for those properties.
        For example, ``['provided_by']``
    edge_facet_properties: Optional[Set]
        A list of edge properties from which to generate counts per value for those properties.
        For example, ``['provided_by']``
    """
    graph_summary(
        inputs,
        input_format,
        input_compression,
        output,
        report_type,
        stream,
        node_facet_properties=list(node_facet_properties),
        edge_facet_properties=list(edge_facet_properties),
    )


@cli.command('validate')
@click.argument('inputs', required=True, type=click.Path(exists=True), nargs=-1)
@click.option(
    '--input-format',
    required=True,
    help=f'The input format. Can be one of {get_input_file_types()}',
)
@click.option('--input-compression', required=False, help='The input compression type')
@click.option(
    '--output',
    required=False,
    type=click.Path(exists=False),
    help='File to write validation reports to',
)
@click.option('--stream', is_flag=True, help='Parse input as a stream')
def validate_wrapper(
    inputs: List[str], input_format: str, input_compression: str, output: str, stream: bool
):
    """
    Run KGX validator on an input file to check for Biolink Model compliance.
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
    stream: bool
        Whether to parse input as a stream

    """
    validate(inputs, input_format, input_compression, output, stream)


@cli.command(name='neo4j-download')
@click.option(
    '--uri',
    required=True,
    type=str,
    help='Neo4j URI to download from. For example, https://localhost:7474',
)
@click.option('--username', required=True, type=str, help='Neo4j username')
@click.option('--password', required=True, type=str, help='Neo4j password')
@click.option('--output', required=True, type=click.Path(exists=False), help='Output')
@click.option(
    '--output-format',
    required=True,
    help=f'The output format. Can be one of {get_input_file_types()}',
)
@click.option('--output-compression', required=False, help='The output compression type')
@click.option('--stream', is_flag=True, help='Parse input as a stream')
@click.option(
    '--node-filters',
    required=False,
    type=click.Tuple([str, str]),
    multiple=True,
    help=f'Filters for filtering nodes from the input graph',
)
@click.option(
    '--edge-filters',
    required=False,
    type=click.Tuple([str, str]),
    multiple=True,
    help=f'Filters for filtering edges from the input graph',
)
def neo4j_download_wrapper(
    uri: str,
    username: str,
    password: str,
    output: str,
    output_format: str,
    output_compression: str,
    stream: bool,
    node_filters: Tuple,
    edge_filters: Tuple,
):
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
        The output type (``tsv``, by default)
    output_compression: str
        The output compression type
    stream: bool
        Whether to parse input as a stream
    node_filters: Tuple[str, str]
        Node filters
    edge_filters: Tuple[str, str]
        Edge filters

    """
    neo4j_download(
        uri,
        username,
        password,
        output,
        output_format,
        output_compression,
        stream,
        node_filters,
        edge_filters,
    )


@cli.command(name='neo4j-upload')
@click.argument('inputs', required=True, type=click.Path(exists=True), nargs=-1)
@click.option(
    '--input-format',
    required=True,
    help=f'The input format. Can be one of {get_input_file_types()}',
)
@click.option('--input-compression', required=False, help='The input compression type')
@click.option(
    '--uri',
    required=True,
    type=str,
    help='Neo4j URI to upload to. For example, https://localhost:7474',
)
@click.option('--username', required=True, type=str, help='Neo4j username')
@click.option('--password', required=True, type=str, help='Neo4j password')
@click.option('--stream', is_flag=True, help='Parse input as a stream')
@click.option(
    '--node-filters',
    required=False,
    type=click.Tuple([str, str]),
    multiple=True,
    help=f'Filters for filtering nodes from the input graph',
)
@click.option(
    '--edge-filters',
    required=False,
    type=click.Tuple([str, str]),
    multiple=True,
    help=f'Filters for filtering edges from the input graph',
)
def neo4j_upload_wrapper(
    inputs: List[str],
    input_format: str,
    input_compression: str,
    uri: str,
    username: str,
    password: str,
    stream: bool,
    node_filters: Tuple[str, str],
    edge_filters: Tuple[str, str],
):
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
    stream: bool
        Whether to parse input as a stream
    node_filters: Tuple[str, str]
        Node filters
    edge_filters: Tuple[str, str]
        Edge filters

    """
    neo4j_upload(
        inputs,
        input_format,
        input_compression,
        uri,
        username,
        password,
        stream,
        node_filters,
        edge_filters,
    )


@cli.command('transform')
@click.argument('inputs', required=False, type=click.Path(exists=True), nargs=-1)
@click.option(
    '--input-format',
    required=False,
    help=f'The input format. Can be one of {get_input_file_types()}',
)
@click.option('--input-compression', required=False, help='The input compression type')
@click.option('--output', required=False, type=click.Path(exists=False), help='Output')
@click.option(
    '--output-format',
    required=False,
    help=f'The output format. Can be one of {get_input_file_types()}',
)
@click.option('--output-compression', required=False, help='The output compression type')
@click.option('--stream', is_flag=True, help='Parse input as a stream')
@click.option(
    '--node-filters',
    required=False,
    type=click.Tuple([str, str]),
    multiple=True,
    help=f'Filters for filtering nodes from the input graph',
)
@click.option(
    '--edge-filters',
    required=False,
    type=click.Tuple([str, str]),
    multiple=True,
    help=f'Filters for filtering edges from the input graph',
)
@click.option('--transform-config', required=False, type=str, help=f'Transform config YAML')
@click.option(
    '--source', required=False, type=str, multiple=True, help='Source(s) from the YAML to process'
)
@click.option('--processes', required=False, type=int, default=1, help='Number of processes to use')
def transform_wrapper(
    inputs: List[str],
    input_format: str,
    input_compression: str,
    output: str,
    output_format: str,
    output_compression: str,
    stream: bool,
    node_filters: Tuple,
    edge_filters: Tuple,
    transform_config: str,
    source: List,
    processes: int,
):
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
    transform_config: str
        Transform config YAML
    source: List
        A list of source(s) to load from the YAML
    processes: int
        Number of processes to use

    """
    transform(
        inputs,
        input_format,
        input_compression,
        output,
        output_format,
        output_compression,
        stream,
        node_filters,
        edge_filters,
        transform_config,
        source,
        processes=processes,
    )


@cli.command(name='merge')
@click.option('--merge-config', required=True, type=str)
@click.option(
    '--source', required=False, type=str, multiple=True, help='Source(s) from the YAML to process'
)
@click.option(
    '--destination',
    required=False,
    type=str,
    multiple=True,
    help='Destination(s) from the YAML to process',
)
@click.option('--processes', required=False, type=int, default=1, help='Number of processes to use')
def merge_wrapper(merge_config: str, source: List, destination: List, processes: int):
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
    source: List
        A list of source to load from the YAML
    destination: List
        A list of destination to write to, as defined in the YAML
    processes: int
        Number of processes to use

    """
    merge(merge_config, source, destination, processes)
