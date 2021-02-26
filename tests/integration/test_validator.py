import os

from kgx.validator import Validator
from kgx.graph.nx_graph import NxGraph
from kgx.transformer import Transformer
from tests import RESOURCE_DIR


def test_validator_bad():
    """
    A fake test to establish a fail condition for validation.
    """
    G = NxGraph()
    G.add_node('x', foo=3)
    G.add_node('ZZZ:3', **{'nosuch': 1})
    G.add_edge('x', 'y', **{'baz': 6})
    validator = Validator(verbose=True)
    e = validator.validate(G)
    assert len(e) > 0


def test_validator_good():
    """
    A fake test to establish a success condition for validation.
    """
    G = NxGraph()
    G.add_node('UniProtKB:P123456', id='UniProtKB:P123456', name='fake', category=['Protein'])
    G.add_node('UBERON:0000001', id='UBERON:0000001', name='fake', category=['NamedThing'])
    G.add_node('UBERON:0000002', id='UBERON:0000002', name='fake', category=['NamedThing'])
    G.add_edge(
        'UBERON:0000001',
        'UBERON:0000002',
        id='UBERON:0000001-part_of-UBERON:0000002',
        relation='RO:1',
        predicate='part_of',
        subject='UBERON:0000001',
        object='UBERON:0000002',
        category=['biolink:Association'],
    )
    validator = Validator(verbose=True)
    e = validator.validate(G)
    print(validator.report(e))
    assert len(e) == 0


def test_validate_json():
    """
    Validate against a valid representative Biolink Model compliant JSON.
    """
    input_args = {'filename': [os.path.join(RESOURCE_DIR, 'valid.json')], 'format': 'json'}
    t = Transformer()
    t.transform(input_args)
    validator = Validator()
    e = validator.validate(t.store.graph)
    assert len(e) == 0
