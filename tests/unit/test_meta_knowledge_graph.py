import json
import os
from sys import stderr
from typing import List, Dict

from kgx import GraphEntityType
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


def test_generate_meta_knowledge_graph_by_stream_inspector():
    """
    Test generate the meta knowledge graph by streaming
    graph data through a graph Transformer.process() Inspector
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

    # We configure the Transformer with a data flow inspector
    # (Deployed in the internal Transformer.process() call)
    transformer.transform(input_args=input_args, inspector=mkg)

    assert mkg.get_name() == 'Test Graph - Streamed'
    assert mkg.get_total_nodes_count() == 512
    assert mkg.get_number_of_categories() == 8
    assert mkg.get_total_edges_count() == 540
    assert mkg.get_edge_mapping_count() == 13
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

    class ProgressMonitor:

        def __init__(self):
            self.count: Dict[GraphEntityType, int] = {
                GraphEntityType.GRAPH: 0,
                GraphEntityType.NODE: 0,
                GraphEntityType.EDGE: 0
            }

        def __call__(self, entity_type: GraphEntityType, rec: List):
            self.count[GraphEntityType.GRAPH] += 1
            self.count[entity_type] += 1
            if not (self.count[GraphEntityType.GRAPH] % 100):
                print(str(self.count[GraphEntityType.GRAPH]) + " records processed...", file=stderr)

        def summary(self):
            print(str(self.count[GraphEntityType.NODE]) + " nodes seen.", file=stderr)
            print(str(self.count[GraphEntityType.EDGE]) + " edges seen.", file=stderr)
            print(str(self.count[GraphEntityType.GRAPH]) + " total records processed...", file=stderr)

    monitor = ProgressMonitor()

    mkg = MetaKnowledgeGraph(
        name='Test Graph - Streamed, Stats accessed via File',
        progress_monitor=monitor
    )

    t.transform(input_args=input_args, inspector=mkg)

    output_filename = os.path.join(TARGET_DIR, 'test_meta_knowledge_graph-2.json')
    with open(output_filename, 'w') as mkgh:
        mkg.save(mkgh)

    data = json.load(open(output_filename))
    assert data['name'] == 'Test Graph - Streamed, Stats accessed via File'
    assert 'NCBIGene' in data['nodes']['biolink:Gene']['id_prefixes']
    assert 'REACT' in data['nodes']['biolink:Pathway']['id_prefixes']
    assert 'HP' in data['nodes']['biolink:PhenotypicFeature']['id_prefixes']
    assert data['nodes']['biolink:Gene']['count'] == 178
    assert len(data['nodes']) == 8
    assert len(data['edges']) == 13

    monitor.summary()


def test_meta_knowledge_graph_multiple_category_and_predicate_parsing():
    """
    Test meta knowledge graph parsing multiple categories
    """
    input_args = {
        'filename': [
            os.path.join(RESOURCE_DIR, 'graph_multi_category_nodes.tsv'),
            os.path.join(RESOURCE_DIR, 'graph_multi_category_edges.tsv'),
        ],
        'format': 'tsv',
    }

    t = Transformer(stream=True)

    mkg = MetaKnowledgeGraph(
        name='Test Graph - Multiple Node Categories'
    )

    t.transform(input_args=input_args, inspector=mkg)

    assert mkg.get_name() == 'Test Graph - Multiple Node Categories'

    assert mkg.get_total_nodes_count() == 10

    # unique set, including (shared) parent
    # classes (not including category 'unknown' )
    assert mkg.get_number_of_categories() == 7

    assert mkg.get_node_count_by_category("biolink:Disease") == 1
    assert mkg.get_node_count_by_category("biolink:BiologicalEntity") == 5
    assert mkg.get_node_count_by_category("biolink:AnatomicalEntityEntity") == 0

    # sums up all the counts of node mappings across
    # all categories (not including category 'unknown')
    assert mkg.get_total_node_counts_across_categories() == 35

    # only counts 'valid' edges for which
    # subject and object nodes are in the nodes file
    assert mkg.get_total_edges_count() == 8

    # total number of distinct predicates
    assert mkg.get_predicate_count() == 2

    # counts edges with a given predicate
    # (ignoring edges with unknown subject or object identifiers)
    assert mkg.get_edge_count_by_predicate("biolink:has_phenotype") == 4
    assert mkg.get_edge_count_by_predicate("biolink:involved_in") == 0

    assert mkg.get_edge_mapping_count() == 25

    assert mkg.get_total_edge_counts_across_mappings() == 100
