import os

from kgx import PandasTransformer
from kgx import ObanRdfTransformer
from kgx import Validator
import kgx.mapper as mapper
import networkx as nx
from random import random
import logging

def write_errors(validator):
    for e in validator.errors:
        print("E={}".format(e))

def test_validator_rdf():
    """
    use test files
    """
    cwd = os.path.abspath(os.path.dirname(__file__))
    resdir = os.path.join(cwd, 'resources')
    src_path = os.path.join(resdir, 'monarch', 'biogrid_test.ttl')
    t = ObanRdfTransformer()
    t.parse(src_path, input_format="turtle")
    validator = Validator()
    validator.validate(t.graph)
    write_errors(validator)
    assert validator.ok()

def test_validator_bad():
    """
    fake test
    """
    G = nx.MultiDiGraph()
    G.add_node('x', foo=3)
    G.add_node('ZZZ:3', nosuch=1)
    G.add_edge('x', 'y', baz=6)
    validator = Validator()
    validator.validate(G)
    write_errors(validator)
    # TODO: status is True when it should be False
    assert not validator.ok()

def test_validator_good():
    """
    fake test
    """
    print("Creating fake test graph...")
    G = nx.MultiDiGraph()
    G.add_node('UniProtKB:P123456', name='fake')
    G.add_edge('UBERON:0000001', 'UBERON:0000002', relation='RO:1', edge_label='part_of')
    validator = Validator()
    #print("PM={}".format(validator.prefix_manager.prefixmap))
    validator.validate(G)
    write_errors(validator)
    assert validator.ok()
