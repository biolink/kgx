import kgx
import os
import pathlib

_transformers = {
    'tar' : kgx.PandasTransformer,
    'txt' : kgx.PandasTransformer,
    'csv' : kgx.PandasTransformer,
    'tsv' : kgx.PandasTransformer,
    'graphml' : kgx.GraphMLTransformer,
    'ttl' : kgx.ObanRdfTransformer,
    'json' : kgx.JsonTransformer,
    'rq' : kgx.SparqlTransformer
}

def is_writable(filepath):
    """
    Checks that the filepath is writable, creating a directory tree if requried
    """
    output = os.path.abspath(filepath)
    dirname = os.path.dirname(filepath)

    pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)

    is_writable = os.access(output, os.W_OK)
    is_creatable = not os.path.isfile(output) and os.access(dirname, os.W_OK)

    return is_writable or is_creatable

def get_transformer(extention):
    return _transformers.get(extention)

def get_file_types():
    return tuple(_transformers.keys())

def get_type(filename):
    for t in _transformers.keys():
        if filename.endswith('.' + t):
            return t
    else:
        return None

class Config(object):
    def __init__(self):
        self.debug = False
