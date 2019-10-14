import kgx
import os, sys, click, logging, itertools, pickle, json, yaml
import pandas as pd
from typing import List
from urllib.parse import urlparse
from kgx import Transformer, map_graph, Filter, FilterLocation
from kgx.validator import Validator
from kgx.cli.error_logging import append_errors_to_file, append_errors_to_files

from kgx.cli.decorators import handle_exception
from kgx.cli.utils import get_file_types, get_type, get_transformer, is_writable

from kgx.cli.utils import Config
from kgx.utils import file_write

from neo4j.v1 import GraphDatabase
from neo4j.v1.types import Node, Record

from collections import Counter, defaultdict, OrderedDict
from terminaltables import AsciiTable

import pandas as pd

from datetime import datetime

pass_config = click.make_pass_decorator(Config, ensure=True)

def error(msg):
    click.echo(msg)
    quit()

@click.group()
@click.option('--debug', is_flag=True, help='Prints the stack trace if error occurs')
@click.version_option(version=kgx.__version__, prog_name=kgx.__name__)
@pass_config
def cli(config, debug):
    """
    Knowledge Graph Exchange
    """
    config.debug = debug
    if debug:
        logging.basicConfig(level=logging.DEBUG)

def get_prefix(curie:str) -> str:
    if ':' in curie:
        prefix, _ = curie.split(':', 1)
        return prefix
    else:
        return None

@cli.command('node-summary')
@click.argument('filepath', type=click.Path(exists=True), required=True)
@click.option('--input-type', type=click.Choice(get_file_types()))
@click.option('--max-rows', '-m', type=int, help='The maximum number of rows to return')
@click.option('--output', '-o', type=click.Path(exists=False))
def node_summary(filepath, input_type, max_rows, output):
    """
    Loads and summarizes a knowledge graph node set
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

            if category is not None and len(category) > 1 and 'named thing' in category:
                category.remove('named thing')

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
            file_write(output, '|nodes|: {}'.format(len(g.nodes())))
            file_write(output, '|edges|: {}'.format(len(g.edges())))
            file_write(output, line)
        else:
            click.echo('|nodes|: {}'.format(len(g.nodes())))
            click.echo('|edges|: {}'.format(len(g.edges())))
            click.echo(line)

    tuple_count = OrderedDict(Counter(tuples).most_common(max_rows))

    headers = [['Prefix', 'Category', 'Frequency']]
    rows = [[*k, v] for k, v in tuple_count.items()]
    if output is not None:
        file_write(output, AsciiTable(headers + rows).table, mode='a')
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
        file_write(output, AsciiTable(headers + rows).table, mode='a')
    else:
        click.echo(AsciiTable(headers + rows).table)

    headers = [['Prefixes', 'Frequency']]
    rows = [[k, v] for k, v in prefix_count.items()]

    if output is not None:
        file_write(output, AsciiTable(headers + rows).table, mode='a')
    else:
        click.echo(AsciiTable(headers + rows).table)

def stringify(s):
    if isinstance(s, list):
        if s is not None and len(s) > 1 and 'named thing' in s:
            s.remove('named thing')
        return ", ".join("'{}'".format(c) for c in s)
    elif isinstance(s, str):
        return "'{}'".format(s)
    else:
        return str(s)

@cli.command('edge-summary')
@click.argument('filepath', type=click.Path(exists=True), required=True)
@click.option('--input-type', type=click.Choice(get_file_types()))
@click.option('--max_rows', '-m', type=int, help='The maximum number of rows to return')
@click.option('--output', '-o', type=click.Path(exists=False))
def edge_summary(filepath, input_type, max_rows, output):
    """
    Loads and summarizes a knowledge graph edge set
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
        file_write(output, AsciiTable(headers + rows).table)
    else:
        click.echo(AsciiTable(headers + rows).table)

@cli.command(name='neo4j-node-summary')
@click.option('-a', '--address', type=str, required=True)
@click.option('-u', '--username', type=str, required=True)
@click.option('-p', '--password', type=str, required=True)
@click.option('-o', '--output', type=click.Path(exists=False))
@pass_config
def neo4j_node_summary(config, address, username, password, output=None):
    if output is not None and not is_writable(output):
        error(f'Cannot write to {output}')

    bolt_driver = GraphDatabase.driver(address, auth=(username, password))

    query = """
    MATCH (x) RETURN DISTINCT x.category AS category
    """

    with bolt_driver.session() as session:
        records = session.run(query)

    categories = set()

    for record in records:
        category = record['category']
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
            query = """
            MATCH (x) WHERE x.category = {category} OR {category} IN x.category
            RETURN DISTINCT
                {category} AS category,
                split(x.id, ':')[0] AS prefix,
                COUNT(*) AS frequency
            ORDER BY category, frequency DESC;
            """

            with bolt_driver.session() as session:
                records = session.run(query, category=category)

            for record in records:
                rows.append({
                    'category' : record['category'],
                    'prefix' : record['prefix'],
                    'frequency' : record['frequency']
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
def neo4j_edge_summary(config, address, username, password, output=None):
    if output is not None and not is_writable(output):
        error(f'Cannot write to {output}')

    bolt_driver = GraphDatabase.driver(address, auth=(username, password))

    query = """
    MATCH (x) RETURN DISTINCT x.category AS category
    """

    with bolt_driver.session() as session:
        records = session.run(query)

    categories = set()

    for record in records:
        category = record['category']
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
            with bolt_driver.session() as session:
                records = session.run(query, category1=category1, category2=category2)

                for r in records:
                    rows.append({
                        'subject_category' : r['subject_category'],
                        'object_category' : r['object_category'],
                        'subject_prefix' : r['subject_prefix'],
                        'object_prefix' : r['object_prefix'],
                        'frequency' : r['frequency']
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
@pass_config
def validate(config, path, output, output_dir):
    t = get_transformer(get_type(path))()
    t.parse(path)

    validator = Validator()
    validator.validate(t.graph)

    time = datetime.now()

    if len(validator.errors) == 0:
        click.echo('No errors found')

    else:
        append_errors_to_file(output, validator.errors, time)
        if output_dir is not None:
            append_errors_to_files(output_dir, validator.errors, time)

from neo4jrestclient.client import GraphDatabase as http_gdb
import networkx as nx

@cli.command(name='neo4j-download')
# @click.option('-d', '--directed', is_flag=True, help='Enforces subject -> object edge direction')
# @click.option('-lb', '--labels', type=(click.Choice(FilterLocation.values()), str), multiple=True, help='For filtering on labels. CHOICE: {}'.format(', '.join(FilterLocation.values())))
# @click.option('-pr', '--properties', type=(click.Choice(FilterLocation.values()), str, str), multiple=True, help='For filtering on properties (key value pairs). CHOICE: {}'.format(', '.join(FilterLocation.values())))
@click.option('-a', '--address', type=str, required=True)
@click.option('-u', '--username', type=str)
@click.option('-p', '--password', type=str)
@click.option('--subject-label', type=str,)
@click.option('--object-label', type=str)
@click.option('--edge-type', type=str)
@click.option('--stop-after', type=int, help='Once this many edges are downloaded the application will finish')
@click.option('--page-size', type=int, default=10_000, help='The size of pages to download for each batch')
# @click.option('--start', type=int, default=0)
# @click.option('--end', type=int)
@click.option('-o', '--output', type=click.Path(exists=False), required=True)
@click.option('--output-type', type=click.Choice(get_file_types()))
@pass_config
def neo4j_download(config, page_size, stop_after, subject_label, object_label, edge_type, address, username, password, output, output_type):
    if not is_writable(output):
        try:
            with open(output, 'w+') as f:
                pass
        except:
            error(f'Cannot write to {output}')

    output_transformer = get_transformer(get_type(output))()
    G = output_transformer.graph

    driver = http_gdb(address, username=username, password=password)

    subject_label = ':`{}`'.format(subject_label) if isinstance(subject_label, str) else ''
    object_label = ':`{}`'.format(object_label) if isinstance(object_label, str) else ''
    edge_type = ':`{}`'.format(edge_type) if isinstance(edge_type, str) else ''

    match = 'match (n{})-[e{}]->(m{})'.format(subject_label, edge_type, object_label)

    results = driver.query('{} return count(*)'.format(match))

    click.echo('Using cyper query: {} return n, e, m'.format(match))

    for a, in results:
        size = a
        break

    if size == 0:
        click.echo('No data available')
        quit()

    page_size = 1_000

    skip_flag = False

    with click.progressbar(list(range(0, size, page_size)), label='Downloading {} many edges'.format(size)) as bar:
        for i in bar:
            q = '{} return n, e, m skip {} limit {}'.format(match, i, page_size)
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

    output_transformer.save(output)



    # t.load(is_directed=directed, start=start, end=end)
    # t.report()
    # transform_and_save(t, output, output_type)

def set_transformer_filters(transformer:Transformer, labels:list, properties:list) -> None:
    for location, label in labels:
        if location == FilterLocation.EDGE.value:
            target = '{}_label'.format(location)
            transformer.set_filter(target=target, value=label)
        else:
            target = '{}_category'.format(location)
            transformer.set_filter(target=target, value=label)

    for location, property_name, property_value in properties:
        target = '{}_property'.format(location)
        transformer.set_filter(target=target, value=(property_name, property_value))

def make_neo4j_transformer(address, username, password):
    o = urlparse(address)

    if o.password is None and password is None:
        error('Could not extract the password from the address, please set password argument')
    elif password is None:
        password = o.password

    if o.username is None and username is None:
        error('Could not extract the username from the address, please set username argument')
    elif username is None:
        username = o.username

    return kgx.NeoTransformer(
        host=o.hostname,
        port=o.port,
        username=username,
        password=password
    )

@cli.command(name='neo4j-upload')
@click.option('--input-type', type=click.Choice(get_file_types()))
@click.option('--use-unwind', is_flag=True, help='Loads using UNWIND, which is quicker')
@click.option('-a', '--address', type=str, required=True)
@click.option('-u', '--username', type=str)
@click.option('-p', '--password', type=str)
@click.argument('inputs', nargs=-1, type=click.Path(exists=False), required=True)
@pass_config
def neo4j_upload(config, address, username, password, inputs, input_type, use_unwind):
    t = load_transformer(inputs, input_type)

    neo_transformer = make_neo4j_transformer(address, username, password)
    neo_transformer.graph = t.graph

    if use_unwind:
        neo_transformer.save_with_unwind()
    else:
        neo_transformer.save()

@cli.command()
@click.option('--input-type', type=click.Choice(get_file_types()))
@click.option('--output-type', type=click.Choice(get_file_types()))
@click.option('--mapping', type=str)
@click.option('--preserve', is_flag=True)
@click.argument('inputs', nargs=-1, type=click.Path(exists=False), required=True)
@click.option('-o', '--output', type=click.Path(exists=False), required=True)
@pass_config
def dump(config, inputs, output, input_type, output_type, mapping, preserve):
    """\b
    Transforms a knowledge graph from one representation to another
    """
    if not is_writable(output):
        error(f'Cannot write to {output}')

    t = load_transformer(inputs, input_type)
    if mapping != None:
        path = get_file_path(mapping)
        with click.open_file(path, 'rb') as f:
            d = pickle.load(f)
            click.echo('Performing mapping: ' + mapping)
            map_graph(G=t.graph, mapping=d, preserve=preserve)
    transform_and_save(t, output, output_type)

@cli.command(name='load-mapping')
@click.argument('name', type=str)
@click.argument('csv', type=click.Path())
@click.option('--no-header', is_flag=True, help='Indicates that the given CSV file has no header, so that the first row will not be ignored')
@click.option('--columns', type=(int, int), default=(None, None), required=False, help="The zero indexed input and output columns for the mapping")
@click.option('--show', is_flag=True, help='Shows a small slice of the mapping')
@pass_config
def load_mapping(config, name, csv, columns, no_header, show):
    header = None if no_header else 0
    data = pd.read_csv(csv, header=header)
    source, target = (0, 1) if columns == (None, None) else columns
    d = {row[source] : row[target] for index, row in data.iterrows()}

    if show:
        for key, value in itertools.islice(d.items(), 5):
            click.echo(str(key) + ' : ' + str(value))

    path = get_file_path(name)

    with open(path, 'wb') as f:
        pickle.dump(d, f)
        click.echo('Mapping \'{name}\' saved at {path}'.format(name=name, path=path))

from kgx import clique_merge

@cli.command()
@click.option('--inputs', '-i', required=True, type=click.Path(exists=True), multiple=True)
@click.option('--output', '-o', required=True, type=click.Path(exists=False))
def merge(inputs, output):
    """
    Loads a series of knowledge graphs and merges cliques using `same_as` edges
    as well as `same_as` node properties. The resulting graph will not have any
    `same_as` edges, and the remaining clique leader nodes will have all
    equivalent identifiers in their `same_as` property.
    """
    transformers = []
    output_transformer = get_transformer(get_type(output))()
    graph = None
    for path in inputs:
        construct = get_transformer(get_type(path))
        if construct is None:
            raise Exception('No transformer for {}'.format(path))
        transformers.append(construct())
    for transformer, path in zip(transformers, inputs):
        if graph is None:
            graph = transformer.graph
        else:
            transformer.graph = graph
        transformer.parse(path)
    output_transformer.graph = graph
    output_transformer.graph = clique_merge(output_transformer.graph)
    output_transformer.save(output)

@cli.command(name='load-and-merge')
@click.argument('merge_config', type=str)
@click.option('--destination-uri', type=str)
@click.option('--destination-username', type=str)
@click.option('--destination-password', type=str)
def load_and_merge(merge_config, destination_uri, destination_username, destination_password):
    """
    Load nodes and edges from KGs, as defined in a config YAML, and merge them into a single graph
    """

    with open(merge_config, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    transformers = []
    for key in cfg['target']:
        logging.info("Connecting to {}".format(cfg['target'][key]))
        uri = "{}:{}".format(cfg['target'][key]['neo4j']['host'], cfg['target'][key]['neo4j']['port'])
        n = kgx.NeoTransformer(None, uri, cfg['target'][key]['neo4j']['username'],
                               cfg['target'][key]['neo4j']['password'])
        transformers.append(n)

        if 'target_filter' in cfg['target'][key]:
            for target_filter in cfg['target'][key]['target_filter']:
                # Set filters
                n.set_filter(target_filter, cfg['target'][key]['target_filter'][target_filter])

        start = 0
        end = None
        if 'query_limits' in cfg['target'][key]:
            if 'start' in cfg['target'][key]['query_limits']:
                start = cfg['target'][key]['query_limits']['start']
            if 'end' in cfg['target'][key]['query_limits']:
                end = cfg['target'][key]['query_limits']['end']

        n.load(start=start, end=end)

    mergedTransformer = Transformer()
    mergedTransformer.merge([x.graph for x in transformers])

    if destination_uri and destination_username and destination_password:
        destination = kgx.NeoTransformer(mergedTransformer.graph, uri=destination_uri, username=destination_username, password=destination_password)
        destination.save_with_unwind()

def get_file_path(name:str) -> str:
    app_dir = click.get_app_dir(__name__)

    if not os.path.exists(app_dir):
        os.makedirs(app_dir)

    return os.path.join(app_dir, name + '.pkl')

def transform_and_save(t:Transformer, output_path:str, output_type:str=None):
    """
    Creates a transformer with the appropraite file type from the given
    transformer, and applies that new transformation and saves to file.
    """
    if output_type is None:
        output_type = get_type(output_path)

    output_transformer = get_transformer(output_type)

    if output_transformer is None:
        error('Output does not have a recognized type: ' + str(get_file_types()))

    kwargs = {
        'extention' : output_type
    }

    w = output_transformer(t.graph)
    result_path = w.save(output_path, **kwargs)

    if result_path is not None and os.path.isfile(result_path):
        click.echo("File created at: " + result_path)
    elif os.path.isfile(output_path):
        click.echo("File created at: " + output_path)
    else:
        error("Could not create file.")

def build_transformer(path:str, input_type:str=None) -> Transformer:
    if input_type is None:
        input_type = get_type(path)
    constructor = get_transformer(input_type)
    if constructor is None:
        error('File does not have a recognized type: ' + str(get_file_types()))
    return constructor()

def load_transformer(input_paths:List[str], input_type:str=None) -> Transformer:
    """
    Creates a transformer for the appropriate file type and loads the data into
    it from file.
    """
    if input_type is None:
        input_types = [get_type(i) for i in input_paths]
        for t in input_types:
            if input_types[0] != t:
                error(
                """
                Each input file must have the same file type.
                Try setting the --input-type parameter to enforce a single
                type.
                """
                )
            input_type = input_types[0]

    transformer_constructor = get_transformer(input_type)

    if transformer_constructor is None:
        error('Inputs do not have a recognized type: ' + str(get_file_types()))

    t = transformer_constructor()
    for i in input_paths:
        t.parse(i, input_type)

    t.report()

    return t
