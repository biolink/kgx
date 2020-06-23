from __future__ import absolute_import

import yaml
from os import path

__version__ = '0.0.1'

from kgx.transformers.pandas_transformer import PandasTransformer
from kgx.transformers.nx_transformer import GraphMLTransformer
from kgx.transformers.rdf_transformer import RdfTransformer, ObanRdfTransformer, RdfOwlTransformer
from kgx.transformers.nt_transformer import NtTransformer
from kgx.transformers.sparql_transformer import SparqlTransformer, RedSparqlTransformer
from kgx.transformers.json_transformer import JsonTransformer, ObographJsonTransformer
from kgx.transformers.rsa_transformer import RsaTransformer
from kgx.transformers.neo_transformer import NeoTransformer
from kgx.transformers.logicterm_transformer import LogicTermTransformer
from kgx.transformers.transformer import Transformer
from .filter import Filter, FilterLocation, FilterType

from kgx.validator import Validator
from kgx.prefix_manager import PrefixManager

import logging

from kgx.config import get_config

config = get_config()
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(config['logging']['format'])
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(config['logging']['level'])
