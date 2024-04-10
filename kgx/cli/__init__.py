from sys import exit
from typing import List, Tuple, Optional, Dict
import click

import kgx
from kgx.config import get_logger, get_config
from kgx.cli.cli_utils import (
    get_input_file_types,
    get_output_file_types,
    parse_source,
    apply_operations,
    graph_summary,
    validate,
    neo4j_download,
    neo4j_upload,
    transform,
    merge,
    summary_report_types,
    get_report_format_types,
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


@cli.command(name="graph-summary")
@click.argument("inputs", required=True, type=click.Path(exists=True), nargs=-1)
@click.option(
    "--input-format",
    "-i",
    required=True,
    help=f"The input format. Can be one of {get_input_file_types()}",
)
@click.option(
    "--input-compression", "-c", required=False, help="The input compression type"
)
@click.option("--output", "-o", required=True, type=click.Path(exists=False))
@click.option(
    "--report-type",
    "-r",
    required=False,
    type=str,
    help=f"The summary get_errors type. Must be one of {tuple(summary_report_types.keys())}",
    default="kgx-map",
)
@click.option(
    "--report-format",
    "-f",
    help=f"The input format. Can be one of {get_report_format_types()}",
)
@click.option(
    "--graph-name",
    "-n",
    required=False,
    help="User specified name of graph being summarized (default: 'Graph')",
)
@click.option(
    "--node-facet-properties",
    required=False,
    multiple=True,
    help="A list of node properties from which to generate counts per value for those properties",
)
@click.option(
    "--edge-facet-properties",
    required=False,
    multiple=True,
    help="A list of edge properties from which to generate counts per value for those properties",
)
@click.option(
    "--error-log",
    "-l",
    required=False,
    type=click.Path(exists=False),
    help='File within which to get_errors graph data parsing errors (default: "stderr")',
)
def graph_summary_wrapper(
    inputs: List[str],
    input_format: str,
    input_compression: Optional[str],
    output: Optional[str],
    report_type: str,
    report_format: str,
    graph_name: str,
    node_facet_properties: Optional[List],
    edge_facet_properties: Optional[List],
    error_log: str = ''
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
    input_compression: Optional[str]
        The input compression type
    output: Optional[str]
        Where to write the output (stdout, by default)
    report_type: str
        The summary get_errors type: "kgx-map" or "meta-knowledge-graph"
    report_format: Optional[str]
        The summary get_errors format file types: 'yaml' or 'json'  (default is report_type specific)
    graph_name: str
        User specified name of graph being summarize
    node_facet_properties: Optional[List]
        A list of node properties from which to generate counts per value for those properties.
        For example, ``['provided_by']``
    edge_facet_properties: Optional[List]
        A list of edge properties from which to generate counts per value for those properties.
        For example, ``['original_knowledge_source', 'aggregator_knowledge_source']``
    error_log: str
        Where to write any graph processing error message (stderr, by default, for empty argument)
    """
    try:
        graph_summary(
            inputs,
            input_format,
            input_compression,
            output,
            report_type,
            report_format,
            graph_name,
            node_facet_properties=list(node_facet_properties),
            edge_facet_properties=list(edge_facet_properties),
            error_log=error_log,
        )
        exit(0)
    except Exception as gse:
        get_logger().error(f"kgx.graph_summary error: {str(gse)}")
        exit(1)


@cli.command(name="validate")
@click.argument("inputs", required=True, type=click.Path(exists=True), nargs=-1)
@click.option(
    "--input-format",
    "-i",
    required=True,
    help=f"The input format. Can be one of {get_input_file_types()}",
)
@click.option(
    "--input-compression", "-c", required=False, help="The input compression type"
)
@click.option(
    "--output",
    "-o",
    required=False,
    type=click.Path(exists=False),
    help="File to write validation reports to",
)
@click.option(
    "--biolink-release",
    "-b",
    required=False,
    help="Biolink Model Release (SemVer) used for validation (default: latest Biolink Model Toolkit version)",
)
def validate_wrapper(
    inputs: List[str],
    input_format: str,
    input_compression: str,
    output: str,
    biolink_release: str = None,
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
    biolink_release: Optional[str]
        SemVer version of Biolink Model Release used for validation (default: latest Biolink Model Toolkit version)
    """
    errors = []
    try:
        errors = validate(
            inputs, input_format, input_compression, output, biolink_release
        )
    except Exception as ex:
        get_logger().error(str(ex))
        exit(2)
    
    if errors:
        get_logger().error("kgx.validate() errors encountered... check the error log")
        exit(1)
    else:
        exit(0)


@cli.command(name="neo4j-download")
@click.option(
    "--uri",
    "-l",
    required=True,
    type=str,
    help="Neo4j URI to download from. For example, https://localhost:7474",
)
@click.option("--username", "-u", required=True, type=str, help="Neo4j username")
@click.option("--password", "-p", required=True, type=str, help="Neo4j password")
@click.option(
    "--output", "-o", required=True, type=click.Path(exists=False), help="Output"
)
@click.option(
    "--output-format",
    "-f",
    required=True,
    help=f"The output format. Can be one of {get_output_file_types()}",
)
@click.option(
    "--output-compression", "-d", required=False, help="The output compression type"
)
@click.option("--stream", "-s", is_flag=True, help="Parse input as a stream")
@click.option(
    "--node-filters",
    "-n",
    required=False,
    type=click.Tuple([str, str]),
    multiple=True,
    help=f"Filters for filtering nodes from the input graph",
)
@click.option(
    "--edge-filters",
    "-e",
    required=False,
    type=click.Tuple([str, str]),
    multiple=True,
    help=f"Filters for filtering edges from the input graph",
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
    try:
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
        exit(0)
    except Exception as nde:
        get_logger().error(f"kgx.neo4j_download error: {str(nde)}")
        exit(1)


@cli.command(name="neo4j-upload")
@click.argument("inputs", required=True, type=click.Path(exists=True), nargs=-1)
@click.option(
    "--input-format",
    "-i",
    required=True,
    help=f"The input format. Can be one of {get_input_file_types()}",
)
@click.option(
    "--input-compression", "-c", required=False, help="The input compression type"
)
@click.option(
    "--uri",
    "-l",
    required=True,
    type=str,
    help="Neo4j URI to upload to. For example, https://localhost:7474",
)
@click.option("--username", "-u", required=True, type=str, help="Neo4j username")
@click.option("--password", "-p", required=True, type=str, help="Neo4j password")
@click.option("--stream", "-s", is_flag=True, help="Parse input as a stream")
@click.option(
    "--node-filters",
    "-n",
    required=False,
    type=click.Tuple([str, str]),
    multiple=True,
    help=f"Filters for filtering nodes from the input graph",
)
@click.option(
    "--edge-filters",
    "-e",
    required=False,
    type=click.Tuple([str, str]),
    multiple=True,
    help=f"Filters for filtering edges from the input graph",
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
    try:
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
        exit(0)
    except Exception as nue:
        get_logger().error(f"kgx.neo4j_upload error: {str(nue)}")
        exit(1)


@cli.command(name="transform")
@click.argument("inputs", required=False, type=click.Path(exists=True), nargs=-1)
@click.option(
    "--input-format",
    "-i",
    required=False,
    help=f"The input format. Can be one of {get_input_file_types()}",
)
@click.option(
    "--input-compression", "-c", required=False, help="The input compression type"
)
@click.option(
    "--output", "-o", required=False, type=click.Path(exists=False), help="Output"
)
@click.option(
    "--output-format",
    "-f",
    required=False,
    help=f"The output format. Can be one of {get_input_file_types()}",
)
@click.option(
    "--output-compression", "-d", required=False, help="The output compression type"
)
@click.option("--stream", is_flag=True, help="Parse input as a stream")
@click.option(
    "--node-filters",
    "-n",
    required=False,
    type=click.Tuple([str, str]),
    multiple=True,
    help=f"Filters for filtering nodes from the input graph",
)
@click.option(
    "--edge-filters",
    "-e",
    required=False,
    type=click.Tuple([str, str]),
    multiple=True,
    help=f"Filters for filtering edges from the input graph",
)
@click.option(
    "--transform-config", required=False, type=str, help=f"Transform config YAML"
)
@click.option(
    "--source",
    required=False,
    type=str,
    multiple=True,
    help="Source(s) from the YAML to process",
)
@click.option(
    "--knowledge-sources",
    "-k",
    required=False,
    type=click.Tuple([str, str]),
    multiple=True,
    help="A named knowledge source with (string, boolean or tuple rewrite) specification",
)
@click.option(
    "--infores-catalog",
    required=False,
    type=click.Path(exists=False),
    help="Optional dump of a CSV file of InfoRes CURIE to Knowledge Source mappings",
)
@click.option(
    "--processes",
    "-p",
    required=False,
    type=int,
    default=1,
    help="Number of processes to use",
)
def transform_wrapper(
    inputs: List[str],
    input_format: str,
    input_compression: str,
    output: str,
    output_format: str,
    output_compression: str,
    stream: bool,
    node_filters: Optional[List[Tuple[str, str]]],
    edge_filters: Optional[List[Tuple[str, str]]],
    transform_config: str,
    source: Optional[List],
    knowledge_sources: Optional[List[Tuple[str, str]]],
    processes: int,
    infores_catalog: Optional[str] = None,
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
        The output compression typ
    stream: bool
        Whether or not to stream
    node_filters: Optional[List[Tuple[str, str]]]
        Node input filters
    edge_filters: Optional[List[Tuple[str, str]]]
        Edge input filters
    transform_config: str
        Transform config YAML
    source: List
        A list of source(s) to load from the YAML
    knowledge_sources: Optional[List[Tuple[str, str]]]
        A list of named knowledge sources with (string, boolean or tuple rewrite) specification
    infores_catalog: Optional[str]
        Optional dump of a TSV file of InfoRes CURIE to Knowledge Source mappings
    processes: int
        Number of processes to use

    """
    try:
        transform(
            inputs,
            input_format=input_format,
            input_compression=input_compression,
            output=output,
            output_format=output_format,
            output_compression=output_compression,
            stream=stream,
            node_filters=node_filters,
            edge_filters=edge_filters,
            transform_config=transform_config,
            source=source,
            knowledge_sources=knowledge_sources,
            processes=processes,
            infores_catalog=infores_catalog,
        )
        exit(0)
    except Exception as te:
        get_logger().error(f"kgx.transform error: {str(te)}")
        exit(1)


@cli.command(name="merge")
@click.option("--merge-config", required=True, type=str)
@click.option(
    "--source",
    required=False,
    type=str,
    multiple=True,
    help="Source(s) from the YAML to process",
)
@click.option(
    "--destination",
    required=False,
    type=str,
    multiple=True,
    help="Destination(s) from the YAML to process",
)
@click.option(
    "--processes",
    "-p",
    required=False,
    type=int,
    default=1,
    help="Number of processes to use",
)
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
    try:
        merge(merge_config, source, destination, processes)
        exit(0)
    except Exception as me:
        get_logger().error(f"kgx.merge error: {str(me)}")
        exit(1)
