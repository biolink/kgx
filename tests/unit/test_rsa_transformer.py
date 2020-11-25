import os

import pytest

from kgx import RsaTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')


def test_load():
    t = RsaTransformer()
    t.parse(os.path.join(resource_dir, 'rsa_sample.json'))
    assert t.graph.number_of_nodes() == 4
    assert t.graph.number_of_edges() == 3

    n = t.graph.nodes()['HGNC:11603']
    assert n['id'] == 'HGNC:11603'
    assert n['name'] == 'TBX4'
    assert n['category'] == ['biolink:Gene']

    e = t.graph.get_edge('HGNC:11603', 'MONDO:0005002')
    data = e.popitem()[1]
    print(data)
    assert data['subject'] == 'HGNC:11603'
    assert data['object'] == 'MONDO:0005002'
    assert data['edge_label'] == 'biolink:related_to'


@pytest.mark.skip('To be implemented')
def test_save():
    pass
