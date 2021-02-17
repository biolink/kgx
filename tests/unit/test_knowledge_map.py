import json
import os

from kgx.operations.knowledge_map import generate_knowledge_map

from kgx import PandasTransformer
from tests import RESOURCE_DIR, TARGET_DIR


def test_generate_knowledge_map():
    t = PandasTransformer()
    t.parse(os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'))
    t.parse(os.path.join(RESOURCE_DIR, 'graph_edges.tsv'))
    output = os.path.join(TARGET_DIR, 'test_graph_knowledge_map.json')
    generate_knowledge_map(t.graph, 'Test Graph', output)

    data = json.load(open(output))
    assert data['name'] == 'Test Graph'
    assert 'NCBIGene' in data['knowledge_map']['nodes']['id_prefixes']
    assert 'REACT' in data['knowledge_map']['nodes']['id_prefixes']
    assert 'HP' in data['knowledge_map']['nodes']['id_prefixes']
    assert data['knowledge_map']['nodes']['count'] == 512
    assert len(data['knowledge_map']['edges']) == 13
