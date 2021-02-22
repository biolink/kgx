import os
import pytest

from kgx.transformer import Transformer
from tests import print_graph, RESOURCE_DIR, TARGET_DIR
from tests.integration import clean_slate, DEFAULT_NEO4J_URL, DEFAULT_NEO4J_USERNAME, DEFAULT_NEO4J_PASSWORD


def _transform(query):
    t1 = Transformer()
    t1.transform(query[0])
    print_graph(t1.store.graph)
    t1.save(query[1].copy())

    assert t1.store.graph.number_of_nodes() == query[2]
    assert t1.store.graph.number_of_edges() == query[3]

    output = query[1]
    if output['format'] in {'tsv', 'csv', 'jsonl'}:
        input_args = {
            'filename': [
                f"{output['filename']}_nodes.{output['format']}",
                f"{output['filename']}_edges.{output['format']}"
            ],
            'format': output['format']
        }
    elif output['format'] in {'neo4j'}:
        input_args = {
            'uri': DEFAULT_NEO4J_URL,
            'username': DEFAULT_NEO4J_USERNAME,
            'password': DEFAULT_NEO4J_PASSWORD,
            'format': 'neo4j'
        }
    else:
        input_args = {
            'filename': [f"{output['filename']}"],
            'format': output['format']
        }

    t2 = Transformer()
    t2.transform(input_args)
    print_graph(t2.store.graph)

    assert t2.store.graph.number_of_nodes() == query[2]
    assert t2.store.graph.number_of_edges() == query[3]


@pytest.mark.parametrize('query', [
    (
        {
            'filename': [
                os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
                os.path.join(RESOURCE_DIR, 'graph_edges.tsv')
            ],
            'format': 'tsv'
        },
        {
            'filename': os.path.join(TARGET_DIR, 'graph1'),
            'format': 'json'
        },
        512,
        532
    ),
    (
        {
            'filename': [
                os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
                os.path.join(RESOURCE_DIR, 'graph_edges.tsv')
            ],
            'format': 'tsv'
        },
        {
            'filename': os.path.join(TARGET_DIR, 'graph2'),
            'format': 'jsonl'
        },
        512,
        532
    ),
    (
        {
            'filename': [
                os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
                os.path.join(RESOURCE_DIR, 'graph_edges.tsv')
            ],
            'format': 'tsv',
            'lineterminator': None,
        },
        {
            'filename': os.path.join(TARGET_DIR, 'graph3.nt'),
            'format': 'nt'
        },
        512,
        532
    ),
    # (
    #     {
    #         'filename': [
    #             os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
    #             os.path.join(RESOURCE_DIR, 'graph_edges.tsv')
    #         ],
    #         'format': 'tsv'
    #     },
    #     {
    #         'uri': DEFAULT_NEO4J_URL,
    #         'username': DEFAULT_NEO4J_USERNAME,
    #         'password': DEFAULT_NEO4J_PASSWORD,
    #         'format': 'neo4j'
    #     },
    #     512,
    #     532
    # ),
    (
        {
            'filename': [
                os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
                os.path.join(RESOURCE_DIR, 'graph_edges.tsv')
            ],
            'format': 'tsv',
            'node_filters': {'category': {'biolink:Gene'}}
        },
        {
            'filename': os.path.join(TARGET_DIR, 'graph2'),
            'format': 'jsonl'
        },
        178,
        178
    ),
    (
        {
            'filename': [
                os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
                os.path.join(RESOURCE_DIR, 'graph_edges.tsv')
            ],
            'format': 'tsv',
            'node_filters': {'category': {'biolink:Gene'}},
            'edge_filters': {'predicate': {'biolink:interacts_with'}}
        },
        {
            'filename': os.path.join(TARGET_DIR, 'graph3'),
            'format': 'jsonl'
        },
        178,
        165
    ),
    (
        {
            'filename': [
                os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
                os.path.join(RESOURCE_DIR, 'graph_edges.tsv')
            ],
            'format': 'tsv',
            'edge_filters': {
                'subject_category': {'biolink:Disease'},
                'object_category': {'biolink:PhenotypicFeature'},
                'predicate': {'biolink:has_phenotype'}
            }
        },
        {
            'filename': os.path.join(TARGET_DIR, 'graph4'),
            'format': 'jsonl'
        },
        133,
        13
    ),
    (
        {
            'filename': [
                os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
                os.path.join(RESOURCE_DIR, 'graph_edges.tsv')
            ],
            'format': 'tsv',
        },
        {
            'filename': os.path.join(TARGET_DIR, 'graph5'),
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


# @pytest.mark.parametrize('query', [
#     (
#         {
#             'filename': [os.path.join(RESOURCE_DIR, 'goslim_generic.owl')],
#             'format': 'owl',
#         },
#         {
#             'uri': DEFAULT_NEO4J_URL,
#             'username': DEFAULT_NEO4J_USERNAME,
#             'password': DEFAULT_NEO4J_PASSWORD,
#             'format': 'neo4j'
#         },
#         220,
#         1050
#     ),
# ])
# def test_transform2(clean_slate, query):
#     _transform(query)
