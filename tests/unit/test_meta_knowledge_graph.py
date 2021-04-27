import json
import os

from kgx.graph_operations.meta_knowledge_graph import generate_knowledge_map
from kgx.transformer import Transformer

from tests import RESOURCE_DIR, TARGET_DIR


def test_generate_knowledge_map():
    """
    Test generate knowledge map operation.
    """
    input_args = {
        'filename': [
            os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
            os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
        ],
        'format': 'tsv',
    }
    t = Transformer()
    t.transform(input_args)
    output = os.path.join(TARGET_DIR, 'test_meta_knowledge_graph.json')
    generate_knowledge_map(t.store.graph, 'Test Graph', output)

    data = json.load(open(output))
    assert data['name'] == 'Test Graph'
    assert 'NCBIGene' in data['nodes']['biolink:Gene']['id_prefixes']
    assert 'REACT' in data['nodes']['biolink:Pathway']['id_prefixes']
    assert 'HP' in data['nodes']['biolink:PhenotypicFeature']['id_prefixes']
    assert data['nodes']['biolink:Gene']['count'] == 178
    assert len(data['edges']) == 13
