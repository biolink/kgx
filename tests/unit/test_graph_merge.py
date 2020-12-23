from kgx.graph.nx_graph import NxGraph
from kgx.operations.graph_merge import merge_all_graphs, merge_graphs, add_all_nodes, merge_node, merge_edge
from tests import print_graph


def get_graphs():
    g1 = NxGraph()
    g1.name = 'Graph 1'
    g1.add_node('A', **{'biolink:id': 'A', 'biolink:name': 'Node A', 'biolink:category': ['biolink:NamedThing']})
    g1.add_node('B', **{'biolink:id': 'B', 'biolink:name': 'Node B', 'biolink:category': ['biolink:NamedThing']})
    g1.add_node('C', **{'biolink:id': 'C', 'biolink:name': 'Node C', 'biolink:category': ['biolink:NamedThing']})
    g1.add_edge('C', 'B', **{'edge_key': 'C-biolink:subclass_of-B', 'biolink:predicate': 'biolink:subclass_of', 'biolink:relation': 'rdfs:subClassOf'})
    g1.add_edge('B', 'A', **{'edge_key': 'B-biolink:subclass_of-A', 'biolink:predicate': 'biolink:subclass_of', 'biolink:relation': 'rdfs:subClassOf', 'biolink:provided_by': 'Graph 1'})

    g2 = NxGraph()
    g2.name = 'Graph 2'
    g2.add_node('A', **{'biolink:id': 'A', 'biolink:name': 'Node A', 'biolink:description': 'Node A in Graph 2', 'biolink:category': ['biolink:NamedThing']})
    g2.add_node('B', **{'biolink:id': 'B', 'biolink:name': 'Node B', 'biolink:description': 'Node B in Graph 2', 'biolink:category': ['biolink:NamedThing']})
    g2.add_node('C', **{'biolink:id': 'C', 'biolink:name': 'Node C', 'biolink:description': 'Node C in Graph 2', 'biolink:category': ['biolink:NamedThing']})
    g2.add_node('D', **{'biolink:id': 'D', 'biolink:name': 'Node D', 'biolink:description': 'Node D in Graph 2', 'biolink:category': ['biolink:NamedThing']})
    g2.add_node('E', **{'biolink:id': 'E', 'biolink:name': 'Node E', 'biolink:description': 'Node E in Graph 2', 'biolink:category': ['biolink:NamedThing']})
    g2.add_edge('B', 'A', **{'edge_key': 'B-biolink:subclass_of-A', 'biolink:predicate': 'biolink:subclass_of', 'biolink:relation': 'rdfs:subClassOf', 'biolink:provided_by': 'Graph 2'})
    g2.add_edge('B', 'A', **{'edge_key': 'B-biolink:related_to-A', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to'})
    g2.add_edge('D', 'A', **{'edge_key': 'D-biolink:related_to-A', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to'})
    g2.add_edge('E', 'A', **{'edge_key': 'E-biolink:related_to-A', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to'})

    g3 = NxGraph()
    g3.name = 'Graph 3'
    g3.add_edge('F', 'E', **{'edge_key': 'F-biolink:same_as-E', 'biolink:predicate': 'biolink:same_as', 'biolink:relation': 'OWL:same_as'})

    return [g1, g2, g3]


def test_merge_all_graphs():
    graphs = get_graphs()
    # merge while preserving conflicting nodes and edges
    merged_graph = merge_all_graphs(graphs, preserve=True)
    assert merged_graph.number_of_nodes() == 6
    assert merged_graph.number_of_edges() == 6
    assert merged_graph.name == 'Graph 2'

    data = merged_graph.nodes()['A']
    assert data['biolink:name'] == 'Node A'
    assert data['biolink:description'] == 'Node A in Graph 2'

    edges = merged_graph.get_edge('B', 'A')
    assert len(edges) == 2

    data = list(edges.values())[0]
    assert len(data['biolink:provided_by']) == 2
    assert data['biolink:provided_by'] == ['Graph 2', 'Graph 1']

    graphs = get_graphs()
    # merge while not preserving conflicting nodes and edges
    merged_graph = merge_all_graphs(graphs, preserve=False)

    assert merged_graph.number_of_nodes() == 6
    assert merged_graph.number_of_edges() == 6
    assert merged_graph.name == 'Graph 2'

    data = merged_graph.nodes()['A']
    assert data['biolink:name'] == 'Node A'
    assert data['biolink:description'] == 'Node A in Graph 2'

    edges = merged_graph.get_edge('B', 'A')
    assert len(edges) == 2

    data = list(edges.values())[0]
    assert isinstance(data['biolink:provided_by'], list)
    assert 'Graph 1' in data['biolink:provided_by']
    assert 'Graph 2' in data['biolink:provided_by']


def test_merge_graphs():
    graphs = get_graphs()
    merged_graph = merge_graphs(NxGraph(), graphs)
    assert merged_graph.number_of_nodes() == 6
    assert merged_graph.number_of_edges() == 6
    assert merged_graph.name not in [x.name for x in graphs]


def test_merge_node():
    graphs = get_graphs()
    g = graphs[0]
    node = g.nodes()['A']
    new_data = node.copy()
    new_data['subset'] = 'test'
    new_data['biolink:source'] = 'KGX'
    new_data['biolink:category'] = ['biolink:InformationContentEntity']
    new_data['biolink:description'] = 'Node A modified by merge operation'
    node = merge_node(g, node['biolink:id'], new_data, preserve=True)

    assert node['biolink:id'] == 'A'
    assert node['biolink:name'] == 'Node A'
    assert node['biolink:description'] == 'Node A modified by merge operation'
    assert 'subset' in node and node['subset'] == 'test'
    assert 'biolink:source' in node and node['biolink:source'] == 'KGX'


def test_merge_edge():
    graphs = get_graphs()
    g = graphs[1]
    edge = g.get_edge('E', 'A')
    new_data = edge.copy()
    new_data['biolink:provided_by'] = 'KGX'
    new_data['biolink:evidence'] = 'PMID:123456'
    edge = merge_edge(g, 'E', 'A', 'E-biolink:related_to-A', new_data, preserve=True)

    assert edge['biolink:predicate'] == 'biolink:related_to'
    assert edge['biolink:relation'] == 'biolink:related_to'
    assert 'KGX' in edge['biolink:provided_by']
    assert edge['biolink:evidence'] == 'PMID:123456'
