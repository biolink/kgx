import kgx
import os, sys, click, logging, itertools, pickle, json, yaml
import pandas as pd
from typing import List

from neo4jrestclient.client import GraphDatabase as http_gdb
from collections import Counter, defaultdict, OrderedDict
from terminaltables import AsciiTable

from kgx.operations.graph_merge import merge_all_graphs
from kgx.validator import Validator
from kgx.cli.utils import get_file_types, get_type, get_transformer, is_writable, build_transformer, get_prefix, \
    stringify, load_transformer, make_neo4j_transformer
from kgx.cli.utils import Config

pass_config = click.make_pass_decorator(Config, ensure=True)


def error(msg):
    logging.error(msg)
    quit()


@click.group()
@click.option('--debug', is_flag=True, help='Prints the stack trace if error occurs')
@click.version_option(version=kgx.__version__, prog_name=kgx.__name__)
@pass_config
def cli(config: dict, debug: bool = False):
    """
    Knowledge Graph Exchange CLI entrypoint.
    \f

    Parameters
    ----------
    config: dict
        A dictionary containing the configuration for kgx.cli
    debug: bool
        Whether to print debug messages

    """
    config.debug = debug
    if debug:
        logging.basicConfig(level=logging.DEBUG)


@cli.command('node-summary')
@click.argument('filepath', type=click.Path(exists=True), required=True)
@click.option('--input-type', type=click.Choice(get_file_types()))
@click.option('--max-rows', '-m', type=int, help='The maximum number of rows to return')
@click.option('--output', '-o', type=click.Path(exists=False))
@pass_config
def node_summary(config: dict, filepath: str, input_type: str, max_rows: int, output: str):
    """
    Loads and summarizes a knowledge graph node set, where the input is a file.
    \f

    Parameters
    ----------
    config: dict
        A dictionary containing the configuration for kgx.cli
    filepath: str
        Input file
    input_type: str
        Input file type
    max_rows: int
        Max number of rows to display in the output
    output: str
        Where to write the output (stdout, by default)

    """
    t = build_transformer(filepath, input_type)
    t.parse(filepath)

    g = t.graph

    tuples = []
    xrefs = set()
    with click.progressbar(g.nodes(data=True), label='Reading knowledge graph') as bar:
        for n, data in bar:
            if 'same_as' in data:
                for xref in data['same_as']:
                    xrefs.add(get_prefix(xref))

            category = data.get('category')
            prefix = get_prefix(n)

            if category is not None and len(category) > 1 and 'named_thing' in category:
                category.remove('named_thing')

            if isinstance(category, (list, set)):
                category = ", ".join("'{}'".format(c) for c in category)

            if prefix is not None:
                prefix = "'{}'".format(prefix)

            tuples.append((prefix, category))

    click.echo('|nodes|: {}'.format(len(g.nodes())))
    click.echo('|edges|: {}'.format(len(g.edges())))

    xrefs = [x for x in xrefs if x is not None]
    if len(xrefs) != 0:
        line = 'xref prefixes: {}'.format(', '.join(xrefs))
        if output is not None:
            FH = open(output)
            FH.write('|nodes|: {}'.format(len(g.nodes())))
            FH.write('|edges|: {}'.format(len(g.edges())))
            FH.write(line)
        else:
            click.echo('|nodes|: {}'.format(len(g.nodes())))
            click.echo('|edges|: {}'.format(len(g.edges())))
            click.echo(line)

    tuple_count = OrderedDict(Counter(tuples).most_common(max_rows))

    headers = [['Prefix', 'Category', 'Frequency']]
    rows = [[*k, v] for k, v in tuple_count.items()]
    if output is not None:
        FH.write(AsciiTable(headers + rows).table)
    else:
        click.echo(AsciiTable(headers + rows).table)

    category_count = defaultdict(lambda: 0)
    prefix_count = defaultdict(lambda: 0)

    for (prefix, category), frequency in tuple_count.items():
        category_count[category] += frequency
        prefix_count[prefix] += frequency

    headers = [['Category', 'Frequency']]
    rows = [[k, v] for k, v in category_count.items()]
    if output is not None:
        FH.write(AsciiTable(headers + rows).table)
    else:
        click.echo(AsciiTable(headers + rows).table)

    headers = [['Prefixes', 'Frequency']]
    rows = [[k, v] for k, v in prefix_count.items()]

    if output is not None:
        FH.write(AsciiTable(headers + rows).table)
    else:
        click.echo(AsciiTable(headers + rows).table)


@cli.command('edge-summary')
@click.argument('filepath', type=click.Path(exists=True), required=True)
@click.option('--input-type', type=click.Choice(get_file_types()))
@click.option('--max_rows', '-m', type=int, help='The maximum number of rows to return')
@click.option('--output', '-o', type=click.Path(exists=False))
@pass_config
def edge_summary(config: dict, filepath: str, input_type: str, max_rows: int, output: str):
    """
    Loads and summarizes a knowledge graph edge set, where the input is a file.
    \f

    Parameters
    ----------
    config: dict
        A dictionary containing the configuration for kgx.cli
    filepath: str
        Input file
    input_type: str
        Input file type
    max_rows: int
        Max number of rows to display in the output
    output: str
        Where to write the output (stdout, by default)

    """
    t = build_transformer(filepath, input_type)
    t.parse(filepath)

    g = t.graph

    tuples = []
    with click.progressbar(g.edges(data=True), label='Reading knowledge graph') as bar:
        for s, o, edge_attr in bar:
            subject_attr = g.node[s]
            object_attr = g.node[o]

            subject_prefix = stringify(get_prefix(s))
            object_prefix = stringify(get_prefix(o))

            subject_category = stringify(subject_attr.get('category'))
            object_category = stringify(object_attr.get('category'))
            edge_label = stringify(edge_attr.get('edge_label'))
            relation = stringify(edge_attr.get('relation'))

            tuples.append((subject_prefix, subject_category, edge_label, relation, object_prefix, object_category))

    tuple_count = OrderedDict(Counter(tuples).most_common(max_rows))

    headers = [['Subject Prefix', 'Subject Category', 'Edge Label', 'Relation', 'Object Prefix', 'Object Category', 'Frequency']]
    rows = [[*k, v] for k, v in tuple_count.items()]

    if output is not None:
        FH = open(output)
        FH.write(AsciiTable(headers + rows).table)
    else:
        click.echo(AsciiTable(headers + rows).table)


@cli.command(name='neo4j-node-summary')
@click.option('-a', '--address', type=str, required=True)
@click.option('-u', '--username', type=str, required=True)
@click.option('-p', '--password', type=str, required=True)
@click.option('-o', '--output', type=click.Path(exists=False))
@pass_config
def neo4j_node_summary(config: dict, address: str, username: str, password: str, output: str = None):
    """
    Get a summary of all the nodes in a Neo4j database.
    \f

    Parameters
    ----------
    config: dict
        A dictionary containing the configuration for kgx.cli
    address: str
        The full HTTP address for Neo4j database
    username: str
        Username for authentication
    password: str
        Password for authentication
    output: str
        Where to write the output (stdout, by default)

    """
    if output is not None and not is_writable(output):
        error(f'Cannot write to {output}')

    http_driver = http_gdb(address, username=username, password=password)

    query = """
    MATCH (x) RETURN DISTINCT x.category AS category
    """

    records = http_driver.query(query)
    categories = set()
    for record in records:
        category = record[0]
        if isinstance(category, str):
            categories.add(category)
        elif isinstance(category, (list, set, tuple)):
            categories.update(category)
        elif category is None:
            continue
        else:
            error('Unrecognized value for node.category: {}'.format(category))

    rows = []
    with click.progressbar(categories, length=len(categories)) as bar:
        for category in bar:
            query = f"""
            MATCH (x) WHERE x.category = '{category}' OR '{category}' IN x.category
            RETURN DISTINCT
                '{category}' AS category,
                split(x.id, ':')[0] AS prefix,
                COUNT(*) AS frequency
            ORDER BY category, frequency DESC;
            """
            records = http_driver.query(query)
            for record in records:
                rows.append({
                    'category': record[0],
                    'prefix': record[1],
                    'frequency': record[2]
                })

    df = pd.DataFrame(rows)
    df = df[['category', 'prefix', 'frequency']]

    if output is None:
        click.echo(df)
    else:
        df.to_csv(output, sep='|', header=True)
        click.echo('Saved report to {}'.format(output))


@cli.command(name='neo4j-edge-summary')
@click.option('-a', '--address', type=str, required=True)
@click.option('-u', '--username', type=str, required=True)
@click.option('-p', '--password', type=str, required=True)
@click.option('-o', '--output', type=click.Path(exists=False))
@pass_config
def neo4j_edge_summary(config: dict, address: str, username: str, password: str, output: str = None):
    """
    Get a summary of all the edges in a Neo4j database.
    \f

    Parameters
    ----------
    config: dict
        A dictionary containing the configuration for kgx.cli
    address: str
        The full HTTP address for Neo4j database
    username: str
        Username for authentication
    password: str
        Password for authentication
    output: str
        Where to write the output (stdout, by default)

    """
    if output is not None and not is_writable(output):
        error(f'Cannot write to {output}')

    http_driver = http_gdb(address, username=username, password=password)

    query = """
    MATCH (x) RETURN DISTINCT x.category AS category
    """

    records = http_driver.query(query)
    categories = set()

    for record in records:
        category = record[0]
        if isinstance(category, str):
            categories.add(category)
        elif isinstance(category, (list, set, tuple)):
            categories.update(category)
        elif category is None:
            continue
        else:
            error('Unrecognized value for node.category: {}'.format(category))

    categories = list(categories)

    query = """
    MATCH (n)-[r]-(m)
    WHERE
        (n.category = {category1} OR {category1} IN n.category) AND
        (m.category = {category2} OR {category2} IN m.category)
    RETURN DISTINCT
        {category1} AS subject_category,
        {category2} AS object_category,
        type(r) AS edge_type,
        split(n.id, ':')[0] AS subject_prefix,
        split(m.id, ':')[0] AS object_prefix,
        COUNT(*) AS frequency
    ORDER BY subject_category, object_category, frequency DESC;
    """

    combinations = [(c1, c2) for c1 in categories for c2 in categories]

    rows = []
    with click.progressbar(combinations, length=len(combinations)) as bar:
        for category1, category2 in bar:
            records = http_driver.query(query, params={'category1': category2, 'category2': category2})
            for r in records:
                rows.append({
                    'subject_category': r[0],
                    'object_category': r[1],
                    'subject_prefix': r[3],
                    'object_prefix': r[4],
                    'frequency': r[5]
                })

    df = pd.DataFrame(rows)
    df = df[['subject_category', 'subject_prefix', 'object_category', 'object_prefix', 'frequency']]

    if output is None:
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            click.echo(df)
    else:
        df.to_csv(output, sep='|', header=True)
        click.echo('Saved report to {}'.format(output))

@cli.command()
@click.argument('path', type=click.Path(exists=True))
@click.option('--output', '-o', type=click.Path(exists=False), required=True, help='The path to a text file to append the output to.')
@click.option('--output-dir', '-d', type=click.Path(exists=False), help='The path to a directory to save a series of text files to.')
@click.option('--format', '-f', required=False, help='The input format type')
@pass_config
def validate(config: dict, path: str, output: str, output_dir: str, format: str):
    """
    Run KGX validation on an input file to check for BioLink Model compliance.
    \f

    Parameters
    ----------
    config: dict
        A dictionary containing the configuration for kgx.cli
    path: str
        Path to input file
    output: str
        Path to output file
    output_dir:
        Path to a directory
    format:
        The input format

    """
    t = None
    if format:
        t = get_transformer(format)()
    else:
        t = get_transformer(get_type(path))()
    t.parse(path, input_format=format)
    validator = Validator()
    errors = validator.validate(t.graph)
    validator.write_report(errors, open(output, 'w'))


@cli.command(name='neo4j-download')
@click.option('-a', '--address', type=str, required=True)
@click.option('-u', '--username', type=str, required=True)
@click.option('-p', '--password', type=str, required=True)
@click.option('-o', '--output', type=click.Path(exists=False), required=True)
@click.option('--output-type', type=click.Choice(get_file_types()), default='csv')
@click.option('--subject-label', type=str)
@click.option('--object-label', type=str)
@click.option('--edge-label', type=str)
@click.option('--directed', type=bool, default=False, help='Whether the edges are directed')
@click.option('--stop-after', type=int, help='Once this many edges are downloaded the application will finish')
@click.option('--page-size', type=int, default=10_000, help='The size of pages to download for each batch')
@pass_config
def neo4j_download(config: dict, address: str, username: str, password: str, output: str, output_type: str, subject_label: str, object_label: str, edge_label: str, directed: bool, page_size: int, stop_after: int):
    """
    Download nodes and edges from Neo4j database.
    \f

    Parameters
    ----------
    config: dict
        A dictionary containing the configuration for kgx.cli
    address: str
        The full HTTP address for Neo4j database
    username: str
        Username for authentication
    password: str
        Password for authentication
    output: str
        Where to write the output (stdout, by default)
    output_type: str
        The output type (``csv``, by default)
    subject_label: str
        The label for subject node in an association
    object_label: str
        The label for object node in an association
    edge_label: str
        The label for the edge in an association
    directed: bool
        Whether or not the edge is supposed to be directed (``true``, by default)
    stop_after: int
        The max number of edges to fetch
    page_size: int
        The page size to use while fetching associations from Neo4j (``10000``, by default)

    """
    if not is_writable(output):
        try:
            with open(output, 'w+') as f:
                pass
        except:
            error(f'Cannot write to {output}')

    output_transformer = get_transformer(output_type)()
    G = output_transformer.graph

    driver = http_gdb(address, username=username, password=password)

    subject_label = ':`{}`'.format(subject_label) if isinstance(subject_label, str) else ''
    object_label = ':`{}`'.format(object_label) if isinstance(object_label, str) else ''
    edge_label = ':`{}`'.format(edge_label) if isinstance(edge_label, str) else ''

    if directed:
        query = 'match (n{})-[e{}]->(m{})'.format(subject_label, edge_label, object_label)
    else:
        query = 'match (n{})-[e{}]-(m{})'.format(subject_label, edge_label, object_label)

    results = driver.query('{} return count(*)'.format(query))
    size = [x[0] for x in results][0]
    print("SIZE: {}".format(size))

    if size == 0:
        click.echo('No records found.')
        return

    click.echo('Using cypher query: {} return n, e, m'.format(query))

    page_size = 1_000
    skip_flag = False

    with click.progressbar(list(range(0, size, page_size)), label='Downloading {} many edges'.format(size)) as bar:
        for i in bar:
            q = '{} return n, e, m skip {} limit {}'.format(query, i, page_size)
            results = driver.query(q)
            for n, e, m in results:
                subject_attr = n['data']
                object_attr = m['data']
                edge_attr = e['data']

                if 'id' not in subject_attr or 'id' not in object_attr:
                    if not skip_flag:
                        click.echo('Skipping records that have no id attribute')
                        skip_flag = True
                    continue

                s = subject_attr['id']
                o = object_attr['id']

                if 'edge_label' not in edge_attr:
                    edge_attr['edge_label'] = e['metadata']['type']

                if 'category' not in subject_attr:
                    subject_attr['category'] = n['metadata']['labels']

                if 'category' not in object_attr:
                    object_attr['category'] = m['metadata']['labels']

                if s not in G:
                    G.add_node(s, **subject_attr)
                if o not in G:
                    G.add_node(o, **object_attr)

                G.add_edge(s, o, key=edge_attr['edge_label'], **edge_attr)

            if stop_after is not None and G.number_of_edges() > stop_after:
                break

    output_transformer.save(output, extension=output_type)


@cli.command(name='neo4j-upload')
@click.option('--input-type', type=click.Choice(get_file_types()))
@click.option('--use-unwind', is_flag=True, help='Loads using UNWIND cypher clause, which is quicker')
@click.option('-a', '--address', type=str, required=True)
@click.option('-u', '--username', type=str)
@click.option('-p', '--password', type=str)
@click.argument('inputs', nargs=-1, type=click.Path(exists=False), required=True)
@pass_config
def neo4j_upload(config: dict, address: str, username: str, password: str, inputs: List[str], input_type: str, use_unwind: bool):
    """
    Upload a set of nodes/edges to a Neo4j database.
    \f

    Parameters
    ----------
    config: dict
        A dictionary containing the configuration for kgx.cli
    address: str
        The full HTTP address for Neo4j database
    username: str
        Username for authentication
    password: str
        Password for authentication
    inputs: List[str]
        A list of files that contains nodes/edges
    input_type: str
        The input type
    use_unwind: bool
        Whether or not to use the UNWIND cypher clause. While this is quicker,
        it requires the Neo4j database to support APOC procedures.

    """
    t = load_transformer(inputs, input_type)
    neo_transformer = make_neo4j_transformer(address, username, password)
    neo_transformer.graph = t.graph

    if use_unwind:
        neo_transformer.save_with_unwind()
    else:
        neo_transformer.save()

@cli.command()
@click.option('--input-type', type=click.Choice(get_file_types()))
@click.option('-o', '--output', type=click.Path(exists=False), required=True)
@click.option('--output-type', type=click.Choice(get_file_types()), required=True)
@click.option('--mapping', type=str)
@click.option('--preserve', is_flag=True)
@click.argument('inputs', nargs=-1, type=click.Path(exists=False), required=True)
@pass_config
def transform(config: dict, inputs: List[str], input_type: str, output: str, output_type: str, mapping: str, preserve: bool):
    """
    Transform a Knowledge Graph from one serialization form to another.
    \f

    Parameters
    ----------
    config: dict
        A dictionary containing the configuration for kgx.cli
    inputs: List[str]
        A list of files that contains nodes/edges
    input_type: str
        The input type
    output: str
        The output file
    output_type: str
        The output type
    mapping: str
        A mapping file (TSV) for remapping node identifiers
    preserve: bool
        Whether to preserve old identifiers before remapping

    """
    # load
    input_transformer = load_transformer(inputs, input_type)

    if mapping is not None:
        # remap
        mapping_dictionary = {}
        with open(mapping) as M:
            for line in M:
                element = line.rstrip().split('\t')
                mapping_dictionary[element[0]] = element[1]
        logging.info('Performing remapping based on {}'.format(mapping))
        #map_graph(input_transformer.graph, mapping=mapping_dictionary, preserve=preserve)

    # save
    output_transformer = get_transformer(output_type)
    if output_transformer is None:
        logging.error('Output does not have a recognized type: ' + str(get_file_types()))
    w = output_transformer(input_transformer.graph)
    w.save(output, output_format=output_type)


# @cli.command()
# @click.option('--inputs', '-i', required=True, type=click.Path(exists=True), multiple=True)
# @click.option('--output', '-o', required=True, type=click.Path(exists=False))
# def merge(inputs, output):
#     """
#     Loads a series of knowledge graphs and merges cliques using `same_as` edges
#     as well as `same_as` node properties. The resulting graph will not have any
#     `same_as` edges, and the remaining clique leader nodes will have all
#     equivalent identifiers in their `same_as` property.
#     """
#     transformers = []
#     output_transformer = get_transformer(get_type(output))()
#     graph = None
#     for path in inputs:
#         construct = get_transformer(get_type(path))
#         if construct is None:
#             raise Exception('No transformer for {}'.format(path))
#         transformers.append(construct())
#     for transformer, path in zip(transformers, inputs):
#         if graph is None:
#             graph = transformer.graph
#         else:
#             transformer.graph = graph
#         transformer.parse(path)
#     output_transformer.graph = graph
#     output_transformer.graph = clique_merge(output_transformer.graph)
#     output_transformer.save(output)

@cli.command(name='load-and-merge')
@click.argument('load_config', type=str)
@pass_config
def load_and_merge(config: dict, load_config):
    """
    Load nodes and edges from files and KGs, as defined in a config YAML, and merge them into a single graph.
    The merge happens in-memory. This merged graph can then be written to a local/remote Neo4j instance
    OR be serialized into a file.
    \f

    .. note::
        Everything here is driven by the ``load_config`` YAML.

    Parameters
    ----------
    """
    with open(load_config, 'r') as YML:
        cfg = yaml.load(YML, Loader=yaml.FullLoader)

    transformers = []
    for key in cfg['target']:
        target = cfg['target'][key]
        logging.info("Loading {}".format(key))
        if target['type'] in get_file_types():
            # loading from a file
            transformer = get_transformer(target['type'])()
            if target['type'] in {'tsv', 'neo4j'}:
                # currently supporting filters only for TSV and Neo4j
                if 'filters' in target:
                    filters = target['filters']
                    node_filters = filters['node_filters'] if 'node_filters' in filters else {}
                    edge_filters = filters['edge_filters'] if 'edge_filters' in filters else {}
                    for k, v in node_filters.items():
                        transformer.set_node_filter(k, set(v))
                    for k, v in edge_filters.items():
                        transformer.set_edge_filter(k, set(v))
                    logging.info(f"with node filters: {node_filters}")
                    logging.info(f"with edge filters: {edge_filters}")
            for f in target['filename']:
                transformer.parse(f, input_format=target['type'])
            transformers.append(transformer)
        elif target['type'] == 'neo4j':
            transformer = kgx.NeoTransformer(None, target['uri'], target['username'],  target['password'])
            if 'filters' in target:
                filters = target['filters']
                node_filters = filters['node_filters'] if 'node_filters' in filters else {}
                edge_filters = filters['edge_filters'] if 'edge_filters' in filters else {}
                for k, v in node_filters.items():
                    transformer.set_node_filter(k, set(v))
                for k, v in edge_filters.items():
                    transformer.set_edge_filter(k, set(v))
                logging.info(f"with node filters: {node_filters}")
                logging.info(f"with edge filters: {edge_filters}")
            transformer.load()
            transformers.append(transformer)
        else:
            logging.error("type {} not yet supported for KGX load-and-merge operation.".format(target['type']))

    merged_graph = merge_all_graphs([x.graph for x in transformers])

    destination = cfg['destination']
    if destination['type'] in ['csv', 'tsv', 'ttl', 'json', 'tar']:
        destination_transformer = get_transformer(destination['type'])(merged_graph)
        destination_transformer.save(destination['filename'])
    elif destination['type'] == 'neo4j':
        destination_transformer = kgx.NeoTransformer(merged_graph, uri=destination['uri'], username=destination['username'], password=destination['password'])
        destination_transformer.save()
    else:
        logging.error("type {} not yet supported for KGX load-and-merge operation.".format(destination['type']))
