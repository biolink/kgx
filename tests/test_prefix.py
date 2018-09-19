from kgx import PandasTransformer
from kgx import ObanRdfTransformer
from kgx import PrefixManager
import networkx as nx
import logging

HAS_EVIDENCE_CURIE = 'RO:0002558'
HAS_EVIDENCE_IRI = 'http://purl.obolibrary.org/obo/RO_0002558'

# TODO: sync with context when we switch to identifiers.org
PUB = 'http://www.ncbi.nlm.nih.gov/pubmed/18375391'

def test_prefixmanager():
    M = PrefixManager()
    assert M.contract(HAS_EVIDENCE_IRI) == HAS_EVIDENCE_CURIE
    assert M.expand(HAS_EVIDENCE_CURIE) == HAS_EVIDENCE_IRI
    #assert M.contract(PUB) == 'PMID:18375391'
    #assert M.expand(M.contract(PUB)) == PUB

