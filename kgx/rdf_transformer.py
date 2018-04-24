import pandas as pd
import logging
from .transformer import Transformer

class RdfTransformer(Transformer):
    """
    Transforms to and from RDF

    We support different RDF metamodels, including:

     - OBAN reification (as used in Monarch)
     - RDF reification

    TODO: we will have some of the same logic if we go from a triplestore. How to share this?
    """

class ObanRdfTransformer(RdfTransformer):
    """
    Transforms to and from RDF, assuming OBAN-style modeling
    """

    
