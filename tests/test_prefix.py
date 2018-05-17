from kgx import PandasTransformer
from kgx import ObanRdfTransformer
from kgx import PrefixManager
import networkx as nx
import logging

HAS_EVIDENCE = 'http://purl.obolibrary.org/obo/RO_0002558'
def test_prefixmanager():
    M = PrefixManager()
    assert M.contract(HAS_EVIDENCE) == 'has_evidence'
    assert M.expand('has_evidence') == HAS_EVIDENCE
