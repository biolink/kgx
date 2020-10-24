import os

import networkx as nx
from kgx import PandasTransformer
from kgx.operations.clique_merge import clique_merge

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, 'resources')
target_dir = os.path.join(cwd, 'target')

prefix_prioritization_map = {'biolink:Gene': ['HGNC', 'NCBIGene', 'ENSEMBL', 'OMIM']}


def test_clique_generation():
    """
    Test for generation of cliques
    """
    t = PandasTransformer()
    t.parse(os.path.join(resource_dir, 'cm_nodes.csv'), input_format='csv')
    t.parse(os.path.join(resource_dir, 'cm_edges.csv'), input_format='csv')
    t.report()
    updated_graph, clique_graph = clique_merge(target_graph=t.graph, prefix_prioritization_map=prefix_prioritization_map)
    cliques = list(nx.connected_components(clique_graph))
    assert len(cliques) == 2


def test_clique_merge():
    """
    Test for clique merge (lenient)
    """
    t = PandasTransformer()
    os.makedirs(target_dir, exist_ok=True)
    t.parse(os.path.join(resource_dir, 'cm_nodes.csv'), input_format='csv')
    t.parse(os.path.join(resource_dir, 'cm_edges.csv'), input_format='csv')
    t.report()
    updated_graph, clique_graph = clique_merge(target_graph=t.graph, prefix_prioritization_map=prefix_prioritization_map)
    leaders = nx.get_node_attributes(updated_graph, 'clique_leader')
    leader_list = list(leaders.keys())
    leader_list.sort()
    assert len(leader_list) == 2
    n1 = updated_graph.nodes[leader_list[0]]
    assert n1['election_strategy'] == 'PREFIX_PRIORITIZATION'
    assert 'NCBIGene:100302240' in n1['same_as']
    assert 'ENSEMBL:ENSG00000284458' in n1['same_as']
    n2 = updated_graph.nodes[leader_list[1]]
    assert n2['election_strategy'] == 'PREFIX_PRIORITIZATION'
    assert 'NCBIGene:8202' in n2['same_as']
    assert 'OMIM:601937' in n2['same_as']
    assert 'ENSEMBL:ENSG00000124151' not in n2['same_as']


def test_clique_merge_edge_consolidation():
    """

    """
    t = PandasTransformer()
    os.makedirs(target_dir, exist_ok=True)
    t.parse(os.path.join(resource_dir, 'cm_test2_nodes.tsv'), input_format='tsv')
    t.parse(os.path.join(resource_dir, 'cm_test2_edges.tsv'), input_format='tsv')
    t.report()
    updated_graph, clique_graph = clique_merge(target_graph=t.graph, prefix_prioritization_map=prefix_prioritization_map)
    leaders = nx.get_node_attributes(updated_graph, 'clique_leader')
    leader_list = list(leaders.keys())
    leader_list.sort()
    assert len(leader_list) == 2

    n1 = updated_graph.nodes[leader_list[0]]
    assert n1['election_strategy'] == 'LEADER_ANNOTATION'
    assert 'NCBIGene:100302240' in n1['same_as']
    assert 'ENSEMBL:ENSG00000284458' in n1['same_as']

    n2 = updated_graph.nodes[leader_list[1]]
    assert n2['election_strategy'] == 'LEADER_ANNOTATION'
    assert 'NCBIGene:8202' in n2['same_as']
    assert 'OMIM:601937' in n2['same_as']
    assert 'ENSEMBL:ENSG00000124151' not in n2['same_as']

    e1_incoming = updated_graph.in_edges('HGNC:7670', data=True)
    assert len(e1_incoming) == 3

    e1_outgoing = updated_graph.out_edges('HGNC:7670', data=True)
    assert len(e1_outgoing) == 6
