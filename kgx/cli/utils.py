import kgx, os, pathlib

_transformers = {
    'txt' : kgx.PandasTransformer,
    'csv' : kgx.PandasTransformer,
    'tsv' : kgx.PandasTransformer,
    'graphml' : kgx.GraphMLTransformer,
    'ttl' : kgx.ObanRdfTransformer,
    'json' : kgx.JsonTransformer,
    'rq' : kgx.SparqlTransformer
}

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

def is_writable(path):
    """
    Checks that a path is writable, creating the directories required if they
    do not exist.
    """
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
    file_is_writable = os.access(path, os.W_OK)
    file_does_not_exist_but_dir_is_writable = not os.path.exists(path) and os.access(dirname, os.W_OK)
    return file_is_writable or file_does_not_exist_but_dir_is_writable

class Config(object):
    def __init__(self):
        self.debug = False
