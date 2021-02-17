import os
import pytest

from kgx.transformer import Transformer
from tests import print_graph, RESOURCE_DIR, TARGET_DIR
from tests.integration import clean_slate, DEFAULT_NEO4J_URL, DEFAULT_NEO4J_USERNAME, DEFAULT_NEO4J_PASSWORD


def _transform(query):
    t = Transformer()
    for i in query[0]:
        t.transform(i)
    print_graph(t.store.graph)
    t.save(query[1])
    assert t.store.graph.number_of_nodes() == query[2]
    assert t.store.graph.number_of_edges() == query[3]


@pytest.mark.parametrize('query', [
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
                'format': 'tsv'
            },
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
                'format': 'tsv'
            },
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph1'),
            'format': 'json'
        },
        512,
        532
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
                'format': 'tsv'
            },
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
                'format': 'tsv'
            },
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph1'),
            'format': 'jsonl'
        },
        512,
        532
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
                'format': 'tsv'
            },
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
                'format': 'tsv'
            },
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph1'),
            'format': 'nt'
        },
        512,
        532
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
                'format': 'tsv'
            },
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
                'format': 'tsv'
            },
        ],
        {
            'uri': DEFAULT_NEO4J_URL,
            'username': DEFAULT_NEO4J_USERNAME,
            'password': DEFAULT_NEO4J_PASSWORD,
            'format': 'neo4j'
        },
        512,
        532
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
                'format': 'tsv',
                'node_filters': {'category': {'biolink:Gene'}}
            },
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
                'format': 'tsv',
            },
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph2'),
            'format': 'jsonl'
        },
        178,
        178
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
                'format': 'tsv',
                'node_filters': {'category': {'biolink:Gene'}}
            },
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
                'format': 'tsv',
                'edge_filters': {'predicate': {'biolink:interacts_with'}}
            },
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph3'),
            'format': 'jsonl'
        },
        178,
        165
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
                'format': 'tsv',
                'edge_filters': {
                    'subject_category': {'biolink:Disease'},
                    'object_category': {'biolink:PhenotypicFeature'},
                    'predicate': {'biolink:has_phenotype'}
                }
            },
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
                'format': 'tsv',
                'edge_filters': {
                    'subject_category': {'biolink:Disease'},
                    'object_category': {'biolink:PhenotypicFeature'},
                    'predicate': {'biolink:has_phenotype'}
                }
            },
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph4'),
            'format': 'jsonl'
        },
        133,
        13
    ),
])
def test_transform1(clean_slate, query):
    """
    Test loading data from a TSV source and writing to various sinks.
    """
    _transform(query)
