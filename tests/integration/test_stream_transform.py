import os

import pytest

from kgx.transformer import Transformer
from tests import TARGET_DIR, RESOURCE_DIR
from tests.integration import clean_slate, DEFAULT_NEO4J_URL, DEFAULT_NEO4J_USERNAME, DEFAULT_NEO4J_PASSWORD


def _stream_transform(query):
    t1 = Transformer(stream=True)
    for i in query[0]:
        t1.transform(i)
    t1.save(query[1])

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
            'filename': os.path.join(TARGET_DIR, 'graph1.json'),
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
])
def test_stream1(clean_slate, query):
    """
    Test streaming data from TSV source and writing to various sinks.
    """
    _stream_transform(query)


@pytest.mark.parametrize('query', [
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph.json'),
                'format': 'json'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph1'),
            'format': 'tsv',
            'node_properties': ['id', 'name', 'category', 'taxon'],
            'edge_properties': ['subject', 'predicate', 'object', 'relation', 'provided_by']
        },
        512,
        532
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph.json'),
                'format': 'json'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph2'),
            'format': 'jsonl'
        },
        512,
        532
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph.json'),
                'format': 'json'
            }
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
                'filename': os.path.join(RESOURCE_DIR, 'graph.json'),
                'format': 'json',
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph3.nt'),
            'format': 'nt'
        },
        512,
        532
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'graph.json'),
                'format': 'json',
                'edge_filters': {
                    'subject_category': {'biolink:Disease'},
                    'object_category': {'biolink:PhenotypicFeature'},
                    'predicate': {'biolink:has_phenotype'}
                }
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph5'),
            'format': 'jsonl'
        },
        133,
        13
    ),
])
def test_stream2(clean_slate, query):
    """
    Test streaming data from JSON source and writing to various sinks.
    """
    _stream_transform(query)


@pytest.mark.parametrize('query', [
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'rdf', 'test3.nt'),
                'format': 'nt'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph1'),
            'format': 'tsv',
            'node_properties': ['id', 'name', 'category', 'description', 'provided_by'],
            'edge_properties': ['subject', 'predicate', 'object', 'relation', 'category', 'fusion', 'homology', 'combined_score', 'cooccurrence'],
        },
        7,
        6
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'rdf', 'test3.nt'),
                'format': 'nt'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph2.json'),
            'format': 'json'
        },
        7,
        6
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'rdf', 'test3.nt'),
                'format': 'nt'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph3.jsonl'),
            'format': 'jsonl'
        },
        7,
        6
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'rdf', 'test3.nt'),
                'format': 'nt'
            }
        ],
        {
            'uri': DEFAULT_NEO4J_URL,
            'username': DEFAULT_NEO4J_USERNAME,
            'password': DEFAULT_NEO4J_PASSWORD,
            'format': 'neo4j'
        },
        7,
        6
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'rdf', 'test3.nt'),
                'format': 'nt',
                'edge_filters': {
                    'subject_category': {'biolink:Gene', 'biolink:Protein'},
                    'object_category': {'biolink:Gene', 'biolink:Protein'},
                    'predicate': {'biolink:has_gene_product', 'biolink:interacts_with'}
                }
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph3.jsonl'),
            'format': 'jsonl'
        },
        6,
        3
    ),
])
def test_stream3(clean_slate, query):
    """
    Test streaming data from RDF source and writing to various sinks.
    """
    _stream_transform(query)


@pytest.mark.parametrize('query', [
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'goslim_generic.json'),
                'format': 'obojson'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph1'),
            'format': 'tsv',
            'node_properties': ['id', 'name', 'category', 'description', 'provided_by'],
            'edge_properties': ['subject', 'predicate', 'object', 'relation', 'category'],
        },
        176,
        206
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'goslim_generic.json'),
                'format': 'obojson'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph2'),
            'format': 'jsonl',
        },
        176,
        206
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'goslim_generic.json'),
                'format': 'obojson'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph3.nt'),
            'format': 'nt',
        },
        176,
        206
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'goslim_generic.json'),
                'format': 'obojson'
            }
        ],
        {
            'uri': DEFAULT_NEO4J_URL,
            'username': DEFAULT_NEO4J_USERNAME,
            'password': DEFAULT_NEO4J_PASSWORD,
            'format': 'neo4j'
        },
        176,
        206
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'goslim_generic.json'),
                'format': 'obojson',
                'edge_filters': {
                    'subject_category': {'biolink:BiologicalProcess'},
                    'predicate': {'biolink:subclass_of'}
                }
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph4.jsonl'),
            'format': 'jsonl'
        },
        72,
        73
    ),
])
def test_stream4(clean_slate, query):
    """
    Test streaming data from RDF source and writing to various sinks.
    """
    _stream_transform(query)


@pytest.mark.parametrize('query', [
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'goslim_generic.owl'),
                'format': 'owl'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph1'),
            'format': 'tsv',
            'node_properties': ['id', 'name', 'category', 'description', 'provided_by'],
            'edge_properties': ['subject', 'predicate', 'object', 'relation', 'category'],
        },
        220,
        1050
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'goslim_generic.owl'),
                'format': 'owl'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph2'),
            'format': 'jsonl'
        },
        220,
        1050
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'goslim_generic.owl'),
                'format': 'owl'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph3'),
            'format': 'nt'
        },
        220,
        1050
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'goslim_generic.owl'),
                'format': 'owl'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph3'),
            'format': 'nt'
        },
        220,
        1050
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'goslim_generic.owl'),
                'format': 'owl'
            }
        ],
        {
            'uri': DEFAULT_NEO4J_URL,
            'username': DEFAULT_NEO4J_USERNAME,
            'password': DEFAULT_NEO4J_PASSWORD,
            'format': 'neo4j'
        },
        220,
        1050
    ),
    # Filters not yet implemented in OwlSource
    # (
    #     [
    #         {
    #             'filename': os.path.join(RESOURCE_DIR, 'goslim_generic.owl'),
    #             'format': 'owl',
    #             'edge_filters': {
    #                 'subject_category': {'biolink:BiologicalProcess'},
    #                 'predicate': {'biolink:subclass_of'}
    #             }
    #         }
    #     ],
    #     {
    #         'filename': os.path.join(TARGET_DIR, 'graph4.jsonl'),
    #         'format': 'jsonl'
    #     },
    #     72,
    #     73
    # ),
])
def test_stream5(clean_slate, query):
    """
    Test streaming data from RDF source and writing to various sinks.
    """
    _stream_transform(query)


@pytest.mark.parametrize('query', [
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'rsa_sample.json'),
                'format': 'trapi-json'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph1'),
            'format': 'tsv',
            'node_properties': ['id', 'name', 'category', 'description', 'provided_by'],
            'edge_properties': ['subject', 'predicate', 'object', 'relation', 'category'],
        },
        4,
        3
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'rsa_sample.json'),
                'format': 'trapi-json'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph2.json'),
            'format': 'json',
        },
        4,
        3
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'rsa_sample.json'),
                'format': 'trapi-json'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph3'),
            'format': 'jsonl',
        },
        4,
        3
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'rsa_sample.json'),
                'format': 'trapi-json'
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph4.nt'),
            'format': 'nt',
        },
        4,
        3
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'rsa_sample.json'),
                'format': 'trapi-json'
            }
        ],
        {
            'uri': DEFAULT_NEO4J_URL,
            'username': DEFAULT_NEO4J_USERNAME,
            'password': DEFAULT_NEO4J_PASSWORD,
            'format': 'neo4j'
        },
        4,
        3
    ),
    (
        [
            {
                'filename': os.path.join(RESOURCE_DIR, 'rsa_sample.json'),
                'format': 'trapi-json',
                'edge_filters': {
                    'subject_category': {'biolink:Disease'},
                }
            }
        ],
        {
            'filename': os.path.join(TARGET_DIR, 'graph3'),
            'format': 'jsonl'
        },
        2,
        0
    ),
])
def test_stream6(clean_slate, query):
    """
    Test streaming data from RDF source and writing to various sinks.
    """
    _stream_transform(query)
