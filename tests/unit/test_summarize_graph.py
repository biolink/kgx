import os
import pytest

from kgx.graph.nx_graph import NxGraph
from kgx.graph_operations.summarize_graph import summarize_graph, generate_graph_stats
from tests import TARGET_DIR


def get_graphs():
    """
    Returns instances of defined graphs.
    """
    g1 = NxGraph()
    g1.name = 'Graph 1'
    g1.add_node('A', id='A', name='Node A', category=['biolink:NamedThing'])
    g1.add_node('B', id='B', name='Node B', category=['biolink:NamedThing'])
    g1.add_node('C', id='C', name='Node C', category=['biolink:NamedThing'])
    g1.add_edge(
        'C',
        'B',
        edge_key='C-biolink:subclass_of-B',
        predicate='biolink:sub_class_of',
        relation='rdfs:subClassOf',
    )
    g1.add_edge(
        'B',
        'A',
        edge_key='B-biolink:subclass_of-A',
        predicate='biolink:sub_class_of',
        relation='rdfs:subClassOf',
        provided_by='Graph 1',
    )

    g2 = NxGraph()
    g2.name = 'Graph 2'
    g2.add_node(
        'A', id='A', name='Node A', description='Node A in Graph 2', category=['biolink:NamedThing']
    )
    g2.add_node(
        'B', id='B', name='Node B', description='Node B in Graph 2', category=['biolink:NamedThing']
    )
    g2.add_node(
        'C', id='C', name='Node C', description='Node C in Graph 2', category=['biolink:NamedThing']
    )
    g2.add_node(
        'D', id='D', name='Node D', description='Node D in Graph 2', category=['biolink:NamedThing']
    )
    g2.add_node(
        'E', id='E', name='Node E', description='Node E in Graph 2', category=['biolink:NamedThing']
    )
    g2.add_edge(
        'B',
        'A',
        edge_key='B-biolink:related_to-A',
        predicate='biolink:related_to',
        relation='biolink:related_to',
    )
    g2.add_edge(
        'D',
        'A',
        edge_key='D-biolink:related_to-A',
        predicate='biolink:related_to',
        relation='biolink:related_to',
    )
    g2.add_edge(
        'E',
        'A',
        edge_key='E-biolink:related_to-A',
        predicate='biolink:related_to',
        relation='biolink:related_to',
    )

    g3 = NxGraph()
    g3.name = 'Graph 3'
    g3.add_edge(
        'F',
        'E',
        edge_key='F-biolink:same_as-E',
        predicate='biolink:same_as',
        relation='OWL:same_as',
    )

    return [g1, g2, g3]


def test_generate_graph_stats():
    """
    Test for generating graph stats.
    """
    graphs = get_graphs()
    for g in graphs:
        filename = os.path.join(TARGET_DIR, f"{g.name}_stats.yaml")
        generate_graph_stats(g, g.name, filename)
        assert os.path.exists(filename)


@pytest.mark.parametrize(
    'query',
    [
        (
            get_graphs()[0],
            {
                'node_stats': {
                    'total_nodes': 3,
                    'node_categories': ['biolink:NamedThing'],
                    'count_by_category': {
                        'unknown': {'count': 0},
                        'biolink:NamedThing': {'count': 3},
                    },
                },
                'edge_stats': {'total_edges': 2},
                'predicates': ['biolink:subclass_of'],
                'count_by_predicates': {
                    'unknown': {'count': 0},
                    'biolink:subclass_of': {'count': 2},
                },
                'count_by_spo': {
                    'biolink:NamedThing-biolink:subclass_of-biolink:NamedThing': {'count': 2}
                },
            },
        ),
        (
            get_graphs()[1],
            {
                'node_stats': {
                    'total_nodes': 5,
                    'node_categories': ['biolink:NamedThing'],
                    'count_by_category': {
                        'unknown': {'count': 0},
                        'biolink:NamedThing': {'count': 5},
                    },
                },
                'edge_stats': {
                    'total_edges': 3,
                    'predicates': ['biolink:related_to'],
                    'count_by_predicates': {
                        'unknown': {'count': 0},
                        'biolink:related_to': {'count': 3},
                    },
                    'count_by_spo': {
                        'biolink:NamedThing-biolink:related_to-biolink:NamedThing': {'count': 3}
                    },
                },
            },
        ),
        (
            get_graphs()[2],
            {
                'node_stats': {
                    'total_nodes': 2,
                    'node_categories': [],
                    'count_by_category': {'unknown': {'count': 2}},
                },
                'edge_stats': {
                    'total_edges': 1,
                    'predicates': ['biolink:same_as'],
                    'count_by_predicates': {
                        'unknown': {'count': 0},
                        'biolink:same_as': {'count': 1},
                    },
                    'count_by_spo': {'unknown-biolink:same_as-unknown': {'count': 1}},
                },
            },
        ),
    ],
)
def test_summarize_graph(query):
    """
    Test for generating graph stats, and comparing the resulting stats.
    """
    stats = summarize_graph(query[0])
    for k, v in query[1]['node_stats'].items():
        assert v == stats['node_stats'][k]
    for k, v in query[1]['edge_stats'].items():
        assert v == stats['edge_stats'][k]
