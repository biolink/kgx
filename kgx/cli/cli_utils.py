import importlib
from typing import List, Tuple, Any, Optional
import pathlib

import kgx
from kgx.config import get_logger

_transformers = {
    'tar': kgx.PandasTransformer,
    'tar.gz': kgx.PandasTransformer,
    'tar.bz2': kgx.PandasTransformer,
    'txt': kgx.PandasTransformer,
    'csv': kgx.PandasTransformer,
    'tsv': kgx.PandasTransformer,
    'nt': kgx.NtTransformer,
    'nt.gz': kgx.NtTransformer,
    'ttl': kgx.RdfTransformer,
    'json': kgx.JsonTransformer,
    # 'rq': kgx.SparqlTransformer,
    'owl': kgx.RdfOwlTransformer,
    'rsa': kgx.RsaTransformer
}

log = get_logger()


def get_transformer(file_format) -> Any:
    """
    Get a Transformer corresponding to a given file format.

    .. note::
        This method returns a reference to kgx.Transformer class
        and not an instance of kgx.Transformer class.
        You will have to instantiate the class by calling its constructor.

    Parameters
    ----------
    file_format: str
        File format

    Returns
    -------
    Any
        Reference to kgx.Transformer class corresponding to ``file_format``

    """
    t = _transformers.get(file_format)
    if not t:
        raise TypeError(f"format '{file_format}' is not a supported file type.")
    return t


def get_file_types() -> Tuple:
    """
    Get all file formats supported by KGX.

    Returns
    -------
    Tuple
        A tuple of supported file formats

    """
    return tuple(_transformers.keys())


def get_type(filename) -> str:
    """
    Get the format for a given filename.

    Parameters
    ----------
    filename: str
        The filename

    Returns
    -------
    str
        The file format

    """
    p = pathlib.Path(filename)
    suffixes = [x[1:] for x in p.suffixes]

    if len(suffixes) == 0:
        log.error(f"Cannot infer suffix for file {filename}")
        file_type = None
    elif len(suffixes) == 1:
        s = suffixes[0]
        file_type = s if s in _transformers.keys() else None
    elif len(suffixes) == 2:
        s = '.'.join(suffixes)
        file_type = s if s in _transformers.keys() else None
        if not file_type:
            file_type = suffixes[-1] if suffixes[-1] in _transformers.keys() else None
    else:
        log.warning(f"Ambiguous file name: {filename}")
        s = suffixes[-1]
        file_type = s if s in _transformers.keys() else None
    return file_type


def apply_filters(target: dict, transformer: kgx.Transformer):
    """
    Apply filters as defined in the YAML.

    Parameters
    ----------
    target: dict
        The target from the YAML
    transformer: kgx.Transformer
        The transformer corresponding to the target

    """
    filters = target['filters']
    node_filters = filters['node_filters'] if 'node_filters' in filters else {}
    edge_filters = filters['edge_filters'] if 'edge_filters' in filters else {}
    for k, v in node_filters.items():
        transformer.set_node_filter(k, set(v))
    for k, v in edge_filters.items():
        transformer.set_edge_filter(k, set(v))
    log.info(f"with node filters: {node_filters}")
    log.info(f"with edge filters: {edge_filters}")


def apply_operations(target: dict, transformer: kgx.Transformer):
    """
    Apply operations as defined in the YAML.

    Parameters
    ----------
    target: dict
        The target from the YAML
    transformer: kgx.Transformer
        The transformer corresponding to the target

    """
    operations = target['operations']
    for operation in operations:
        op_name = operation['name']
        op_args = operation['args']
        module_name = '.'.join(op_name.split('.')[0:-1])
        function_name = op_name.split('.')[-1]
        f = getattr(importlib.import_module(module_name), function_name)
        log.info(f"Applying operation {op_name} with args: {op_args}")
        f(transformer.graph, **op_args)
