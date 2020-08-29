import os
from kgx import PandasTransformer
from kgx.operations.graph_merge import merge_all_graphs

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, 'resources')
target_dir = os.path.join(cwd, 'target')

def test_merge():
    """
    Test for merging graphs
    """
    pt1 = PandasTransformer()
    pt1.parse(os.path.join(resource_dir, 'merge', 'test1_nodes.tsv'), input_format='tsv')
    pt1.parse(os.path.join(resource_dir, 'merge', 'test1_edges.tsv'), input_format='tsv')
    pt2 = PandasTransformer()
    pt2.parse(os.path.join(resource_dir, 'merge', 'test2_nodes.tsv'), input_format='tsv')
    pt2.parse(os.path.join(resource_dir, 'merge', 'test2_edges.tsv'), input_format='tsv')
    merged_graph = merge_all_graphs([pt1.graph, pt2.graph], preserve=True)
    assert len(merged_graph.nodes()) == 6
    assert len(merged_graph.edges()) == 8

    x1 = merged_graph.nodes['x1']
    assert x1['name'] == 'node x1'

    assert isinstance(x1['category'], list)
    assert 'a' in x1['p1']
    assert '1' in x1['p1']

    x10 = merged_graph.nodes['x10']
    assert x10['id'] == 'x10'
    assert x10['name'] == 'node x10'

def test_merge_no_preserve():
    """
    Test for merging graphs, overwriting conflicting properties
    """
    pt1 = PandasTransformer()
    pt1.parse(os.path.join(resource_dir, 'merge', 'test1_nodes.tsv'), input_format='tsv')
    pt1.parse(os.path.join(resource_dir, 'merge', 'test1_edges.tsv'), input_format='tsv')
    pt2 = PandasTransformer()
    pt2.parse(os.path.join(resource_dir, 'merge', 'test2_nodes.tsv'), input_format='tsv')
    pt2.parse(os.path.join(resource_dir, 'merge', 'test2_edges.tsv'), input_format='tsv')
    merged_graph = merge_all_graphs([pt1.graph, pt2.graph], preserve=False)
    assert len(merged_graph.nodes()) == 6
    assert len(merged_graph.edges()) == 8

    x1 = merged_graph.nodes['x1']
    print(x1)
    assert x1['name'] == 'node x1'

    assert isinstance(x1['category'], list)
    assert list(pt1.graph.nodes['x1']['category'])[0] in x1['category']
    assert list(pt2.graph.nodes['x1']['category'])[0] in x1['category']
    assert x1['p1'] == 'a'

