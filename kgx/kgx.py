import kgx
import click
import os

_transformers = {
    'txt' : kgx.PandasTransformer,
    'csv' : kgx.PandasTransformer,
    'tsv' : kgx.PandasTransformer,
    'graphml' : kgx.GraphMLTransformer,
    'ttl' : kgx.ObanRdfTransformer,
    'json' : kgx.JsonTransformer,
    'rq' : kgx.SparqlTransformer
}

_file_types = ', '.join(_transformers.keys())

class Config(object):
    def __init__(self):
        self.debug = False

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

@cli.command()
@click.option('--input-type', type=str, help='Extention type of input files: ' + _file_types)
@click.option('--output-type', type=str, help='Extention type of output files: ' + _file_types)
@click.argument('input', nargs=-1, type=click.Path(exists=False))
@click.argument('output', type=click.Path(exists=False))
@pass_config
def dump(config, input, output, input_type, output_type):
    """\b
    Transforms a knowledge graph from one representation to another
    INPUT  : any number of files or endpoints
    OUTPUT : the output file
    """
    try:
        _dump(input, output, input_type, output_type)
    except Exception as e:
        if config.debug:
            raise e
        else:
            raise click.ClickException(e)

def _dump(input, output, input_type, output_type):
    if output_type is None:
        output_type = _get_type(output)

    if input_type is None:
        input_types = [_get_type(i) for i in input]
        for t in input_types:
            if input_types[0] != t:
                raise Exception("""Each input file must have the same file type.
                    Try setting the --input-type parameter to enforce a single
                    type."""
                )
            input_type = input_types[0]

    input_transformer = _transformers.get(input_type)

    if input_transformer is None:
        raise Exception('Input does not have a recognized type: ' + _file_types)

    t = input_transformer()

    for i in input:
        t.parse(i)

    t.report()

    output_transformer = _transformers.get(output_type)

    if output_transformer is None:
        raise Exception('Output does not have a recognized type: ' + _file_types)

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


def _get_type(filename):
    for t in _transformers.keys():
        if filename.endswith('.' + t):
            return t
    else:
        return None
