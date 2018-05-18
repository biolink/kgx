import kgx
import click
import os
import logging

from typing import List
from kgx import Transformer

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

@cli.command(name='neo4j-download')
@click.option('--output-type', type=str, help='Extention type of output files: ' + get_file_types())
@click.argument('uri', type=str)
@click.argument('username', type=str)
@click.argument('password', type=str)
@click.argument('output', type=click.Path(exists=False))
@pass_config
@handle_exception
def neo4j_download(config, uri, username, password, output, output_type):
    if output_type is None:
        output_type = get_type(output)

    neo_transformer = kgx.NeoTransformer(uri=uri, username=username, password=password)

    neo_transformer.load()

    constructor = get_transformer(output_type)

    kwargs = {
        'extention' : output_type
    }

    w = constructor(neo_transformer)
    result_path = w.save(output, **kwargs)

    if result_path is not None and os.path.isfile(result_path):
        click.echo("File created at: " + result_path)
    elif os.path.isfile(output):
        click.echo("File created at: " + output)
    else:
        click.echo("Could not create file.")

@cli.command(name='neo4j-upload')
@click.option('--input-type', type=str, help='Extention type of output files: ' + get_file_types())
@click.argument('uri', type=str)
@click.argument('username', type=str)
@click.argument('password', type=str)
@click.argument('inputs', nargs=-1, type=click.Path(exists=False))
@pass_config
@handle_exception
def neo4j_upload(config, uri, username, password, inputs, input_type):
    t = load_transformer(inputs, input_type)
    neo_transformer = kgx.NeoTransformer(t=t, uri=uri, username=username, password=password)
    neo_transformer.save()

@cli.command()
@click.option('--input-type', type=str, help='Extention type of input files: ' + get_file_types())
@click.option('--output-type', type=str, help='Extention type of output files: ' + get_file_types())
@click.argument('inputs', nargs=-1, type=click.Path(exists=False))
@click.argument('output', type=click.Path(exists=False))
@pass_config
@handle_exception
def dump(config, inputs, output, input_type, output_type):
    """\b
    Transforms a knowledge graph from one representation to another
    INPUTS  : any number of files or endpoints
    OUTPUT : the output file
    """
    if output_type is None:
        output_type = get_type(output)

    if input_type is None:
        input_types = [get_type(i) for i in inputs]
        for t in input_types:
            if input_types[0] != t:
                raise Exception("""Each input file must have the same file type.
                    Try setting the --input-type parameter to enforce a single
                    type."""
                )
            input_type = input_types[0]

    input_transformer = get_transformer(input_type)

    if input_transformer is None:
        raise Exception('Inputs do not have a recognized type: ' + get_file_types())

    t = input_transformer()

    for i in inputs:
        t.parse(i)

    t.report()

    output_transformer = get_transformer(output_type)

    if output_transformer is None:
        raise Exception('Output does not have a recognized type: ' + get_file_types())

    kwargs = {
        'extention' : output_type
    }

    w = output_transformer(t)
    result_path = w.save(output, **kwargs)

    if result_path is not None and os.path.isfile(result_path):
        click.echo("File created at: " + result_path)
    elif os.path.isfile(output):
        click.echo("File created at: " + output)
    else:
        click.echo("Could not create file.")

def transform_and_save(t:Transformer, output_path:str, output_type:str=None):
    """
    Creates a transformer with the appropraite file type from the given
    transformer, and applies that new transformation and saves to file.
    """
    output_transformer = get_transformer(output_type)

    if output_transformer is None:
        raise Exception('Output does not have a recognized type: ' + get_file_types())

    kwargs = {
        'extention' : output_type
    }

    w = output_transformer(t)
    result_path = w.save(output, **kwargs)

    if result_path is not None and os.path.isfile(result_path):
        click.echo("File created at: " + result_path)
    elif os.path.isfile(output):
        click.echo("File created at: " + output)
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
        raise Exception('Inputs do not have a recognized type: ' + get_file_types())

    t = transformer_constructor()

    for i in input_paths:
        t.parse(i)

    t.report()

    return t
