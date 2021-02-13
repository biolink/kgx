import os
from kgx import RsaTransformer
from tests import RESOURCE_DIR


def test_load():
    """
    Test for loading into RsaTransformer
    """
    json_file = os.path.join(RESOURCE_DIR, 'robokop.json')
    rt = RsaTransformer()
    rt.parse(json_file)
    edge_list = list(rt.graph.edges(data=True))
    assert edge_list[0][-1]['subject'] == 'HGNC:30922'
    assert edge_list[0][-1]['object'] == 'MONDO:0000429'
