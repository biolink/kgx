import kgx
import click

_transformers = {
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
@click.version_option(prog_name=kgx.__name__)
@pass_config
def cli(config, debug):
    config.debug = debug

@cli.command()
@click.option('--input-type', type=str, help='Extention type of input files: ' + _file_types)
@click.option('--output-type', type=str, help='Extention type of output files: ' + _file_types)
@click.argument('node', type=click.Path(exists=False))
@click.argument('edge', type=click.Path(exists=False))
@click.argument('output', type=click.Path(exists=False))
@pass_config
def dump(config, node, edge, output, input_type, output_type):
    try:
        _dump(node, edge, output, input_type, output_type)
    except Exception as e:
        if config.debug:
            raise e
        else:
            raise click.ClickException(e)

def _dump(node, edge, output, input_type, output_type):
    if output_type is None:
        output_type = _get_type(output)

    if input_type is None:
        node_input_type = _get_type(node)
        edge_input_type = _get_type(edge)
        if node_input_type != edge_input_type:
            raise click.ClickException('Node and edge file must have the same file extension')
        else:
            input_type = node_input_type

    input_transformer = _transformers.get(input_type)

    if input_transformer is None:
        raise Exception('Node/Edge does not have a recognized type: ' + _file_types)

    t = input_transformer()

    t.parse(node)
    t.parse(edge)

    output_transformer = _transformers.get(output_type)

    if output_transformer is None:
        raise Exception('Output does not have a recognized type: ' + _file_types)

    w = output_transformer(t)

    w.save(output)

def _get_type(filename):
    for t in _transformers.keys():
        if filename.endswith('.' + t):
            return t
    else:
        return None
