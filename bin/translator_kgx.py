import kgx
import click
import os
import logging
import itertools
import pickle
import pandas as pd

from typing import List
from kgx import Transformer
from kgx import Validator
from kgx import map_graph

from kgx.cli.decorators import handle_exception
from kgx.cli.utils import get_file_types, get_type, get_transformer

from kgx.cli.utils import Config

pass_config = click.make_pass_decorator(Config, ensure=True)

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

@cli.command()
@click.option('--input-type', type=click.Choice(get_file_types()))
@click.argument('inputs', nargs=-1, type=click.Path(exists=False))
@pass_config
@handle_exception
def validate(config, inputs, input_type):
    v = Validator()
    t = load_transformer(inputs, input_type)
    result = v.validate(t.graph)
    click.echo(result)

@cli.command(name='neo4j-download')
@click.option('--output-type', type=click.Choice(get_file_types()))
@click.option('--property-filter', type=(str, str), multiple=True)
@click.option('--batch-size', type=int, help='The number of records to save in each file')
@click.option('--batch-start', type=int, help='The index to skip ahead to with: starts at 0')
@click.argument('uri', type=str)
@click.argument('username', type=str)
@click.argument('password', type=str)
@click.argument('output', type=click.Path(exists=False))
@pass_config
@handle_exception
def neo4j_download(config, uri, username, password, output, output_type, property_filter, batch_size, batch_start):
    if batch_start != None and batch_size == None:
        raise Exception('batch-size must be set if batch-start is set')

    if batch_start == None and batch_size != None:
        batch_start = 0

    for key, value in property_filter:
        t.set_filter(key, value)

    if batch_size != None and batch_start >= 0:
        for i in itertools.count(batch_start):
            t = kgx.NeoTransformer(uri=uri, username=username, password=password)
            start = batch_size * i
            end = start + batch_size

            t.load(start=start, end=end)

            if t.is_empty():
                return

            name, extention = output.split('.', 1)
            indexed_output = name + '({}).'.format(i) + extention
            transform_and_save(t, indexed_output, output_type)
    else:
        t = kgx.NeoTransformer(uri=uri, username=username, password=password)
        t.load()
        transform_and_save(t, output, output_type)

@cli.command(name='neo4j-upload')
@click.option('--input-type', type=click.Choice(get_file_types()))
@click.argument('uri', type=str)
@click.argument('username', type=str)
@click.argument('password', type=str)
@click.argument('inputs', nargs=-1, type=click.Path(exists=False))
@pass_config
@handle_exception
def neo4j_upload(config, uri, username, password, inputs, input_type):
    t = load_transformer(inputs, input_type)
    neo_transformer = kgx.NeoTransformer(graph=t.graph, uri=uri, username=username, password=password)
    neo_transformer.save()

@cli.command()
@click.option('--input-type', type=click.Choice(get_file_types()))
@click.option('--output-type', type=click.Choice(get_file_types()))
@click.option('--mapping', type=str)
@click.option('--preserve', is_flag=True)
@click.argument('inputs', nargs=-1, type=click.Path(exists=False))
@click.argument('output', type=click.Path(exists=False))
@pass_config
@handle_exception
def dump(config, inputs, output, input_type, output_type, mapping, preserve):
    """\b
    Transforms a knowledge graph from one representation to another
    INPUTS  : any number of files or endpoints
    OUTPUT : the output file
    """
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
@click.argument('source-col', type=str, required=True)
@click.argument('target-col', type=str, required=True)
@click.argument('csv', type=click.Path())
@pass_config
def load_mapping(config, name, csv, source_col, target_col):
    import pudb; pudb.set_trace()
    data = pd.read_csv(csv)

    d = {row[source_col] : row[target_col] for index, row in data.iterrows()}

    path = get_file_path(name)

    with open(path, 'wb') as f:
        pickle.dump(d, f)
        click.echo('Mapping \'{}\' saved at {}'.format(name, path))


def get_file_path(filename:str) -> str:
    app_dir = click.get_app_dir(__name__)

    if not os.path.exists(app_dir):
        os.makedirs(app_dir)

    return os.path.join(app_dir, filename)

def transform_and_save(t:Transformer, output_path:str, output_type:str=None):
    """
    Creates a transformer with the appropraite file type from the given
    transformer, and applies that new transformation and saves to file.
    """
    if output_type is None:
        output_type = get_type(output_path)

    output_transformer = get_transformer(output_type)

    if output_transformer is None:
        raise Exception('Output does not have a recognized type: ' + str(get_file_types()))

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
        click.echo("Could not create file.")

def load_transformer(input_paths:List[str], input_type:str=None) -> Transformer:
    """
    Creates a transformer for the appropriate file type and loads the data into
    it from file.
    """
    if input_type is None:
        input_types = [get_type(i) for i in input_paths]
        for t in input_types:
            if input_types[0] != t:
                raise Exception("""Each input file must have the same file type.
                    Try setting the --input-type parameter to enforce a single
                    type."""
                )
            input_type = input_types[0]

    transformer_constructor = get_transformer(input_type)

    if transformer_constructor is None:
        raise Exception('Inputs do not have a recognized type: ' + str(get_file_types()))

    t = transformer_constructor()

    for i in input_paths:
        t.parse(i)

    t.report()

    return t
