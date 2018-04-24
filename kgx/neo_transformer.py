import pandas as pd
import logging
from .transformer import Transformer

class NeoTransformer(Transformer):
    """
    TODO: use bolt

    We expect a Translator canonical style http://bit.ly/tr-kg-standard
    E.g. predicates are names with underscores, not IDs.

    TODO: also support mapping from Monarch neo4j
    """
    
