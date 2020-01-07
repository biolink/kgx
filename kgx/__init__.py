from __future__ import absolute_import

import yaml

__version__ = '0.0.1'

from .transformers.pandas_transformer import PandasTransformer
from .transformers.nx_transformer import GraphMLTransformer
from .transformers.rdf_transformer import RdfTransformer, ObanRdfTransformer, RdfOwlTransformer
from .transformers.sparql_transformer import SparqlTransformer, RedSparqlTransformer
from .transformers.json_transformer import JsonTransformer
from .transformers.neo_transformer import NeoTransformer
from .transformers.logicterm_transformer import LogicTermTransformer
from .transformers.transformer import Transformer
from .filter import Filter, FilterLocation, FilterType

from .validator import Validator
from .prefix_manager import PrefixManager
from .mapper import map_graph, clique_merge
from .utils.model_utils import make_valid_types

import logging

CONFIG_FILENAME = "config.yml"
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
        try:
            config = yaml.load(open(filename), Loader=yaml.FullLoader)
            print(config)
        except FileNotFoundError:
            config = {
                'logging': {
                    'format':  '%(levelname)s: %(message)s',
                    'level': 'INFO'
                }
            }

    return config


config = get_config()
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(config['logging']['format'])
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(config['logging']['level'])
