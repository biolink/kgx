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
    
    transformer = Transformer()

    transformer.transform(input_args)

    output_filename = os.path.join(TARGET_DIR, 'test_meta_knowledge_graph-1.json')
    
    generate_meta_knowledge_graph(transformer.store.graph, 'Test Graph', output_filename)

    data = json.load(open(output_filename))
    assert data['name'] == 'Test Graph'
    assert 'NCBIGene' in data['nodes']['biolink:Gene']['id_prefixes']
    assert 'REACT' in data['nodes']['biolink:Pathway']['id_prefixes']
    assert 'HP' in data['nodes']['biolink:PhenotypicFeature']['id_prefixes']
    assert data['nodes']['biolink:Gene']['count'] == 178
    assert len(data['nodes']) == 8
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

    transformer = Transformer(stream=True)

    mkg = MetaKnowledgeGraph('Test Graph - Streamed')

    transformer.transform(input_args=input_args, inspector=mkg)

    assert mkg.get_name() == 'Test Graph - Streamed'
    assert mkg.get_total_nodes_count() == 534
    assert mkg.get_category_count() == 8
    assert mkg.get_total_edges_count() == 540
    assert mkg.get_edge_map_count() == 13
    assert 'NCBIGene' in mkg.get_category('biolink:Gene').get_id_prefixes()
    assert 'REACT' in mkg.get_category('biolink:Pathway').get_id_prefixes()
    assert 'HP' in mkg.get_category('biolink:PhenotypicFeature').get_id_prefixes()
    assert mkg.get_category('biolink:Gene').get_count() == 178


#
# Testing alternate approach of generating and using a meta knowledge graph
#
def test_generate_streaming_meta_knowledge_graph_via_saved_file():
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

    inspector = MetaKnowledgeGraph('Test Graph - Streamed, Stats accessed via File')

    t.transform(input_args=input_args, inspector=inspector)

    output_filename = os.path.join(TARGET_DIR, 'test_meta_knowledge_graph-2.json')
    with open(output_filename, 'w') as mkgh:
        inspector.save(mkgh)

    data = json.load(open(output_filename))
    assert data['name'] == 'Test Graph - Streamed, Stats accessed via File'
    assert 'NCBIGene' in data['nodes']['biolink:Gene']['id_prefixes']
    assert 'REACT' in data['nodes']['biolink:Pathway']['id_prefixes']
    assert 'HP' in data['nodes']['biolink:PhenotypicFeature']['id_prefixes']
    assert data['nodes']['biolink:Gene']['count'] == 178
    assert len(data['nodes']) == 8
    assert len(data['edges']) == 13
