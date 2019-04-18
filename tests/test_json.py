import os
from kgx import JsonTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, 'resources')
target_dir = os.path.join(cwd, 'target')

def test_load():
    """
    Test for loading into JsonTransformer
    """
    json_file = os.path.join(resource_dir, 'semmed/gene.json')
    jt = JsonTransformer()
    jt.parse(json_file)
    edge_list = list(jt.graph.edges(data=True))
    assert edge_list[0][-1]['subject'] == 'UMLS:C0948075'
    assert edge_list[0][-1]['object'] == 'UMLS:C1290952'

def test_export():
    """
    Test export behavior of JsonTransformer
    """
    json_file = os.path.join(resource_dir, 'semmed/gene.json')
    output_file = os.path.join(target_dir, 'semmeddb_export.json')
    jt = JsonTransformer()
    jt.parse(json_file)
    jt.save(output_file)
    assert os.path.isfile(output_file)
