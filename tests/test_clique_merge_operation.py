import os
import networkx as nx
from kgx import PandasTransformer
from kgx.operations.clique_merge import CliqueMerge
import logging

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, 'resources')
target_dir = os.path.join(cwd, 'target')

prefix_prioritization_map = {'biolink:Gene': ['HGNC', 'NCBIGene', 'ENSEMBL', 'OMIM']}

def test_clique_generation():
    """
    Test for generation of cliques
    """
    t = PandasTransformer()
    t.parse(os.path.join(resource_dir, 'cm_nodes.csv'))
    t.parse(os.path.join(resource_dir, 'cm_edges.csv'))
    t.report()
    cm = CliqueMerge(prefix_prioritization_map)
    cm.build_cliques(t.graph)
    cliques = list(nx.connected_components(cm.clique_graph))
    assert len(cliques) == 2

def test_clique_merge():
    """
    Test for clique merge (lenient)
    """
    t = PandasTransformer()
    os.makedirs(target_dir, exist_ok=True)
    t.parse(os.path.join(resource_dir, 'cm_nodes.csv'))
    t.parse(os.path.join(resource_dir, 'cm_edges.csv'))
    t.report()
    cm = CliqueMerge(prefix_prioritization_map)
    cm.build_cliques(t.graph)
    cm.elect_leader()
    updated_graph = cm.consolidate_edges()
    leaders = nx.get_node_attributes(updated_graph, 'clique_leader')
    leader_list = list(leaders.keys())
    leader_list.sort()
    assert len(leader_list) == 2
    n1 = updated_graph.nodes[leader_list[0]]
    assert n1['election_strategy'] == 'PREFIX_PRIORITIZATION'
    assert 'NCBIGene:100302240' in n1['aliases']
    assert 'ENSEMBL:ENSG00000284458' in n1['aliases']
    n2 = updated_graph.nodes[leader_list[1]]
    assert n2['election_strategy'] == 'PREFIX_PRIORITIZATION'
    assert 'NCBIGene:8202' in n2['aliases']
    assert 'OMIM:601937' in n2['aliases']
    assert 'ENSEMBL:ENSG00000124151' not in n2['aliases']


