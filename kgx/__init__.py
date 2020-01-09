from __future__ import absolute_import

import yaml
from os import path

__version__ = '0.0.1'

from kgx.transformers.pandas_transformer import PandasTransformer
from kgx.transformers.nx_transformer import GraphMLTransformer
from kgx.transformers.rdf_transformer import RdfTransformer, ObanRdfTransformer, RdfOwlTransformer
from kgx.transformers.sparql_transformer import SparqlTransformer, RedSparqlTransformer
from kgx.transformers.json_transformer import JsonTransformer
from kgx.transformers.neo_transformer import NeoTransformer
from kgx.transformers.logicterm_transformer import LogicTermTransformer
from kgx.transformers.transformer import Transformer
from .filter import Filter, FilterLocation, FilterType

from .validator import Validator
from .prefix_manager import PrefixManager
from .mapper import map_graph, clique_merge
from .utils.model_utils import make_valid_types

import logging

CONFIG_FILENAME = path.join(path.dirname(path.abspath(__file__)), 'config.yml')
config = None

def get_config(filename: str = CONFIG_FILENAME) -> dict:
    """
    Get config as a dictionary

    Parameters
    ----------
    filename: str
        The filename with all the configuration

    Returns
    -------
    dict
        A dictionary containing all the entries from the config YAML

    """
    global config
    if config is None:
        config = yaml.load(open(filename), Loader=yaml.FullLoader)
    return config


config = get_config()
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(config['logging']['format'])
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(config['logging']['level'])
