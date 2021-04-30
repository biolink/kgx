import json
import os

from kgx.graph_operations.meta_knowledge_graph import generate_meta_knowledge_graph, MetaKnowledgeGraph
from kgx.transformer import Transformer

from tests import RESOURCE_DIR, TARGET_DIR


def test_generate_classical_meta_knowledge_graph():
    """
    Test generate meta knowledge graph operation.
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
    generate_meta_knowledge_graph(t.store.graph, 'Test Graph', output)

    data = json.load(open(output))
    assert data['name'] == 'Test Graph'
    assert 'NCBIGene' in data['nodes']['biolink:Gene']['id_prefixes']
    assert 'REACT' in data['nodes']['biolink:Pathway']['id_prefixes']
    assert 'HP' in data['nodes']['biolink:PhenotypicFeature']['id_prefixes']
    assert data['nodes']['biolink:Gene']['count'] == 178
    assert len(data['edges']) == 13


def test_generate_streaming_meta_knowledge_graph_direct():
    """
    Test generate meta knowledge graph operation...
    MetaKnowledgeGraph as direct Transformer.transform Inspector
    """
    input_args = {
        'filename': [
            os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
            os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
        ],
        'format': 'tsv',
    }

    t = Transformer(stream=True)

    inspector = MetaKnowledgeGraph('Test Graph - Streamed')

    t.transform(input_args=input_args, inspector=inspector)

    assert inspector.get_name() == 'Test Graph - Streamed'
    assert inspector.get_node_count() == 534
    assert inspector.get_edge_count() == 540
    assert 'NCBIGene' in inspector.get_category('biolink:Gene').get_id_prefixes()
    assert 'REACT' in inspector.get_category('biolink:Pathway').get_id_prefixes()
    assert 'HP' in inspector.get_category('biolink:PhenotypicFeature').get_id_prefixes()
    assert inspector.get_category('biolink:Gene').get_count() == 178


#
# Testing alternate approach of generating and using a meta knowledge graph
#
# output = os.path.join(TARGET_DIR, 'test_meta_knowledge_graph.json')
# inspector.save('Test Graph 2', output)
def test_generate_streaming_meta_knowledge_graph_via_file():
    """
    Test generate meta knowledge graph operation...
    MetaKnowledgeGraph as streaming Transformer.transform Inspector
    """
    input_args = {
        'filename': [
            os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
            os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
        ],
        'format': 'tsv',
    }
    t = Transformer(stream=True)

    inspector = MetaKnowledgeGraph('Test Graph - Streamed')

    t.transform(input_args=input_args, inspector=inspector)

    output = os.path.join(TARGET_DIR, 'test_meta_knowledge_graph.json')
    generate_meta_knowledge_graph(t.store.graph, 'Test Graph - Streamed into File', output)

    data = json.load(open(output))
    assert data['name'] == 'Test Graph - Streamed into File'
    assert 'NCBIGene' in data['nodes']['biolink:Gene']['id_prefixes']
    assert 'REACT' in data['nodes']['biolink:Pathway']['id_prefixes']
    assert 'HP' in data['nodes']['biolink:PhenotypicFeature']['id_prefixes']
    assert data['nodes']['biolink:Gene']['count'] == 178
    assert len(data['edges']) == 13
