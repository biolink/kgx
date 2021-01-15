import json
import os

from kgx.operations.knowledge_map import generate_knowledge_map

from kgx import PandasTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')


def test_generate_knowledge_map():
    t = PandasTransformer()
    t.parse(os.path.join(resource_dir, 'graph_nodes.tsv'))
    t.parse(os.path.join(resource_dir, 'graph_edges.tsv'))
    output = os.path.join(target_dir, 'test_graph_knowledge_map.json')
    generate_knowledge_map(t.graph, 'Test Graph', output)

    data = json.load(open(output))
    assert data['name'] == 'Test Graph'
    assert 'NCBIGene' in data['knowledge_map']['nodes']['id_prefixes']
    assert 'REACT' in data['knowledge_map']['nodes']['id_prefixes']
    assert 'HP' in data['knowledge_map']['nodes']['id_prefixes']
    assert data['knowledge_map']['nodes']['count'] == 512
    assert len(data['knowledge_map']['edges']) == 13
