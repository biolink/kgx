import os

import pytest

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

def test_validator_bad():
    """
    fake test
    """
    G = nx.MultiDiGraph()
    G.add_node('x', foo=3)
    G.add_node('ZZZ:3', nosuch=1)
    G.add_edge('x', 'y', baz=6)
    validator = Validator(verbose=True)
    validator.validate(G)
    e = validator.validate(G)
    for x in e:
        print(x)
    assert len(e) > 0


def test_validator_good():
    """
    fake test
    """
    print("Creating fake test graph...")
    G = nx.MultiDiGraph()
    G.add_node('UniProtKB:P123456', id='UniProtKB:P123456', name='fake', category=['Protein'])
    G.add_node('UBERON:0000001', id='UBERON:0000001', name='fake', category=['NamedThing'])
    G.add_node('UBERON:0000002', id='UBERON:0000002', name='fake', category=['NamedThing'])
    G.add_edge('UBERON:0000001', 'UBERON:0000002', association_id='UBERON:0000001-part_of-UBERON:0000002', relation='RO:1', edge_label='part_of', subject='UBERON:0000001', object='UBERON:0000002')
    validator = Validator(verbose=True)
    #print("PM={}".format(validator.prefix_manager.prefixmap))
    e = validator.validate(G)
    for x in e:
        print(x)
    assert len(e) == 0
