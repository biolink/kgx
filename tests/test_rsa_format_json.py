import os
from kgx import RsaTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, 'resources')
target_dir = os.path.join(cwd, 'target')

def test_load():
    """
    Test for loading into RsaTransformer
    """
    json_file = os.path.join(resource_dir, 'robokop.json')
    rt = RsaTransformer()
    rt.parse(json_file)
    edge_list = list(rt.graph.edges(data=True))
    assert edge_list[0][-1]['biolink:subject'] == 'HGNC:30922'
    assert edge_list[0][-1]['biolink:object'] == 'MONDO:0000429'
