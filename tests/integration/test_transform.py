import os
import pytest

from kgx.transformer import Transformer
from tests import print_graph, RESOURCE_DIR, TARGET_DIR
from tests.integration import clean_slate, DEFAULT_NEO4J_URL, DEFAULT_NEO4J_USERNAME, DEFAULT_NEO4J_PASSWORD


def _transform(query):
    t1 = Transformer()
    for i in query[0]:
        t1.transform(i)
    print_graph(t1.store.graph)
    t1.save(query[1].copy())
    assert t1.store.graph.number_of_nodes() == query[2]
    assert t1.store.graph.number_of_edges() == query[3]

    if query[1]['format'] in {'tsv', 'csv', 'jsonl'}:
        input_args = []
        ia1 = {
            'filename': f"{query[1]['filename']}_nodes.{query[1]['format']}",
            'format': query[1]['format']
        }
        ia2 = {
            'filename': f"{query[1]['filename']}_edges.{query[1]['format']}",
            'format': query[1]['format']
        }
        input_args.append(ia1)
        input_args.append(ia2)
    elif query[1]['format'] in {'neo4j'}:
        input_args = [
            {
                'uri': DEFAULT_NEO4J_URL,
                'username': DEFAULT_NEO4J_USERNAME,
                'password': DEFAULT_NEO4J_PASSWORD,
                'format': 'neo4j'
            }
        ]
    else:
        input_args = [
            {
                'filename': query[1]['filename'],
                'format': query[1]['format']
            }
        ]

    t2 = Transformer()
    for i in input_args:
        t2.transform(i)
    assert t2.store.graph.number_of_nodes() == query[2]
    assert t2.store.graph.number_of_edges() == query[3]


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
                'format': 'tsv',
                'lineterminator': None
            },
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
                'format': 'tsv',
                'lineterminator': None
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
            'format': 'tsv'
        },
        512,
        532
    ),
])
def test_transform1(clean_slate, query):
    """
    Test loading data from a TSV source and writing to various sinks.
    """
    _transform(query)
