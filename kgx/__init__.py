from __future__ import absolute_import

__version__ = '0.0.1'

from .pandas_transformer import PandasTransformer
from .nx_transformer import GraphMLTransformer
from .rdf_transformer import RdfTransformer, ObanRdfTransformer, RdfOwlTransformer
from .sparql_transformer import SparqlTransformer, RedSparqlTransformer
from .json_transformer import JsonTransformer
from .neo_transformer import NeoTransformer
from .logicterm_transformer import LogicTermTransformer
from .transformer import Transformer
from .filter import Filter, FilterLocation, FilterType

from .sink import Sink
from .debug_sink import DebugSink
from .progress_sink import ProgressSink
from .csv_sink import CsvSink

from .source import Source
from .neo_source import NeoSource
from .sparql_source import SparqlSource

from .validator import Validator
from .prefix_manager import PrefixManager
from .mapper import map_graph, clique_merge
from .utils.model_utils import make_valid_types

import logging

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(filename)s][%(funcName)20s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
