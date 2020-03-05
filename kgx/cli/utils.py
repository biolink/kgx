import logging
from typing import List

import kgx
import os
import pathlib
from urllib.parse import urlparse

from kgx.transformers.transformer import Transformer

_transformers = {
    'tar': kgx.PandasTransformer,
    'txt': kgx.PandasTransformer,
    'csv': kgx.PandasTransformer,
    'tsv': kgx.PandasTransformer,
    'graphml': kgx.GraphMLTransformer,
    'ttl': kgx.ObanRdfTransformer,
    'json': kgx.JsonTransformer,
    'rq': kgx.SparqlTransformer,
    'owl': kgx.RdfOwlTransformer,
    'rsa': kgx.RsaTransformer
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

def load_transformer(input_paths: List[str], input_type: str = None) -> Transformer:
    """
    Creates a transformer for the appropriate file type and parses the file to load its content.
    .. note:: All files in ``input_paths`` should be of the same type.

    Parameters
    ----------
    input_paths: List[str]
        A list of input file paths
    input_type: str
        Input file type

    Returns
    -------
    kgx.transformers.Transformer
        Returns a `kgx.transformers.Transformer` instance corresponding to the ``input_type``

    """
    if input_type is None:
        input_types = [get_type(i) for i in input_paths]
        for t in input_types:
            if input_types[0] != t:
                logging.error(
                """
                Each input file must have the same file type.
                Try setting the --input-type parameter to enforce a single
                type.
                """
                )
            input_type = input_types[0]

    transformer_constructor = get_transformer(input_type)
    if transformer_constructor is None:
        logging.error('Inputs do not have a recognized type: ' + str(get_file_types()))

    t = transformer_constructor()
    for i in input_paths:
        t.parse(i, input_type)

    t.report()
    return t

def build_transformer(path: str, input_type: str = None) -> Transformer:
    """
    Creates a transformer for the appropriate input file type.

    Parameters
    ----------
    path: str
        The path to a file
    input_type: str
        Input file type

    Returns
    -------
    kgx.transformers.Transformer
        Returns a `kgx.transformers.Transformer` instance corresponding to the ``input_type``

    """
    if input_type is None:
        input_type = get_type(path)
    constructor = get_transformer(input_type)
    if constructor is None:
        logging.error('File does not have a recognized type: ' + str(get_file_types()))
    return constructor()


def make_neo4j_transformer(address, username, password):
    o = urlparse(address)

    if o.password is None and password is None:
        logging.error('Could not extract the password from the address, please set password argument')
    elif password is None:
        password = o.password

    if o.username is None and username is None:
        logging.error('Could not extract the username from the address, please set username argument')
    elif username is None:
        username = o.username

    return kgx.NeoTransformer(
        host=o.hostname,
        port=o.port,
        username=username,
        password=password
    )


def stringify(s):
    if isinstance(s, list):
        if s is not None and len(s) > 1 and 'named_thing' in s:
            s.remove('named_thing')
        return ", ".join("'{}'".format(c) for c in s)
    elif isinstance(s, str):
        return "'{}'".format(s)
    else:
        return str(s)


def get_prefix(curie:str) -> str:
    if ':' in curie:
        prefix, _ = curie.split(':', 1)
        return prefix
    else:
        return None

