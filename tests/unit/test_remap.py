import pprint
import pytest

from kgx.graph.nx_graph import NxGraph
from kgx.utils.graph_utils import remap_node_identifier, remap_node_property, remap_edge_property
from tests import print_graph


def get_graphs():
    g1 = NxGraph()
    g1.name = 'Graph 1'
    g1.add_node('HGNC:12345', **{'biolink:id': 'HGNC:12345', 'biolink:name': 'Test Gene', 'biolink:category': ['biolink:NamedThing'], 'alias': 'NCBIGene:54321', 'biolink:same_as': 'UniProtKB:54321'})
    g1.add_node('B', **{'biolink:id': 'B', 'biolink:name': 'Node B', 'biolink:category': ['biolink:NamedThing'], 'alias': 'Z'})
    g1.add_node('C', **{'biolink:id': 'C', 'biolink:name': 'Node C', 'biolink:category': ['biolink:NamedThing']})
    g1.add_edge('C', 'B', **{'edge_key': 'C-biolink:subclass_of-B', 'biolink:subject': 'C', 'biolink:object': 'B', 'biolink:predicate': 'biolink:subclass_of', 'biolink:relation': 'rdfs:subClassOf', 'biolink:provided_by': 'Graph 1', 'biolink:publication': [1], 'pubs': ['PMID:123456']})
    g1.add_edge('B', 'A', **{'edge_key': 'B-biolink:subclass_of-A', 'biolink:subject': 'B', 'biolink:object': 'A', 'biolink:predicate': 'biolink:subclass_of', 'biolink:relation': 'rdfs:subClassOf', 'biolink:provided_by': 'Graph 1'})

    g2 = NxGraph()
    g2.name = 'Graph 2'
    g2.add_node('A', **{'biolink:id': 'A', 'biolink:name': 'Node A', 'biolink:description': 'Node A in Graph 2', 'biolink:category': ['biolink:Gene'], 'biolink:xref': ['NCBIGene:12345', 'HGNC:001033']})
    g2.add_node('B', **{'biolink:id': 'B', 'biolink:name': 'Node B', 'biolink:description': 'Node B in Graph 2', 'biolink:category': ['biolink:Gene'], 'biolink:xref': ['NCBIGene:56463', 'HGNC:012901']})
    g2.add_node('C', **{'biolink:id': 'C', 'biolink:name': 'Node C', 'biolink:description': 'Node C in Graph 2', 'biolink:category': ['biolink:Gene', 'biolink:NamedThing'], 'biolink:xref': ['NCBIGene:08239', 'HGNC:103431']})
    g2.add_node('D', **{'biolink:id': 'D', 'biolink:name': 'Node D', 'biolink:description': 'Node D in Graph 2', 'biolink:category': ['biolink:Gene'], 'biolink:xref': ['HGNC:394233']})
    g2.add_node('E', **{'biolink:id': 'E', 'biolink:name': 'Node E', 'biolink:description': 'Node E in Graph 2', 'biolink:category': ['biolink:NamedThing'], 'biolink:xref': ['NCBIGene:X', 'HGNC:X']})
    g2.add_node('F', **{'biolink:id': 'F', 'biolink:name': 'Node F', 'biolink:description': 'Node F in Graph 2', 'biolink:category': ['biolink:NamedThing'], 'biolink:xref': ['HGNC:Y']})
    g2.add_edge('B', 'A', **{'edge_key': 'B-biolink:subclass_of-A', 'biolink:subject': 'B', 'biolink:object': 'A', 'biolink:predicate': 'biolink:subclass_of', 'biolink:relation': 'rdfs:subClassOf', 'biolink:provided_by': 'Graph 2'})
    g2.add_edge('B', 'A', **{'edge_key': 'B-biolink:related_to-A', 'biolink:subject': 'B', 'biolink:object': 'A', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to'})
    g2.add_edge('D', 'A', **{'edge_key': 'D-biolink:related_to-A', 'biolink:subject': 'D', 'biolink:object': 'A', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to'})
    g2.add_edge('E', 'A', **{'edge_key': 'E-biolink:related_to-A', 'biolink:subject': 'E', 'biolink:object': 'A', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to'})
    g2.add_edge('E', 'F', **{'edge_key': 'F-biolink:related_to-A', 'biolink:subject': 'E', 'biolink:object': 'F', 'biolink:predicate': 'biolink:related_to', 'biolink:relation': 'biolink:related_to'})

    return [g1, g2]


def test_remap_node_identifier_alias():
    graphs = get_graphs()
    g = remap_node_identifier(graphs[0], 'biolink:NamedThing', alternative_property='alias')
    pprint.pprint([x for x in g.nodes(data=True)])
    assert g.has_node('NCBIGene:54321')
    assert g.has_node('Z')
    assert g.has_node('C')
    assert g.has_edge('C', 'Z')
    assert g.has_edge('Z', 'A')
    assert not g.has_edge('C', 'B')
    assert not g.has_edge('B', 'A')

    e1 = list(g.get_edge('C', 'Z').values())[0]
    print(e1)
    assert e1['biolink:subject'] == 'C' and e1['biolink:object'] == 'Z'
    assert e1['edge_key'] == 'C-biolink:subclass_of-Z'

    e2 = list(g.get_edge('Z', 'A').values())[0]
    assert e2['biolink:subject'] == 'Z' and e2['biolink:object'] == 'A'
    assert e2['edge_key'] == 'Z-biolink:subclass_of-A'


def test_remap_node_identifier_xref():
    graphs = get_graphs()
    g = remap_node_identifier(graphs[1], 'biolink:Gene', alternative_property='biolink:xref', prefix='NCBIGene')
    assert g.has_node('NCBIGene:12345')
    assert g.has_node('NCBIGene:56463')
    assert g.has_node('NCBIGene:08239')
    assert g.has_node('D')
    assert g.has_node('E')
    assert g.has_node('F')
    assert not g.has_node('A')
    assert not g.has_node('B')
    assert not g.has_node('C')

    e1 = list(g.get_edge('NCBIGene:56463', 'NCBIGene:12345').values())[0]
    assert e1['biolink:subject'] == 'NCBIGene:56463' and e1['biolink:object'] == 'NCBIGene:12345'

    e2 = list(g.get_edge('D', 'NCBIGene:12345').values())[0]
    assert e2['biolink:subject'] == 'D' and e2['biolink:object'] == 'NCBIGene:12345'

    e3 = list(g.get_edge('E', 'NCBIGene:12345').values())[0]
    assert e3['biolink:subject'] == 'E' and e3['biolink:object'] == 'NCBIGene:12345'

    e4 = list(g.get_edge('E', 'F').values())[0]
    assert e4['biolink:subject'] == 'E' and e4['biolink:object'] == 'F'


def test_remap_node_property():
    graphs = get_graphs()
    remap_node_property(graphs[0], category='biolink:NamedThing', old_property='alias', new_property='biolink:same_as')
    assert graphs[0].nodes()['HGNC:12345']['alias'] == 'UniProtKB:54321'


def test_remap_node_property_fail():
    graphs = get_graphs()
    with pytest.raises(AttributeError):
        remap_node_property(graphs[0], category='biolink:NamedThing', old_property='biolink:id', new_property='alias')


def test_remap_edge_property():
    graphs = get_graphs()
    remap_edge_property(graphs[0], edge_predicate='biolink:subclass_of', old_property='biolink:publication', new_property='pubs')
    e = list(graphs[0].get_edge('C', 'B').values())[0]
    assert e['biolink:publication'] == ['PMID:123456']


def test_remap_edge_property_fail():
    graphs = get_graphs()
    with pytest.raises(AttributeError):
        remap_edge_property(graphs[0], edge_predicate='biolink:subclass_of', old_property='biolink:subject', new_property='pubs')

    with pytest.raises(AttributeError):
        remap_edge_property(graphs[0], edge_predicate='biolink:subclass_of', old_property='biolink:object', new_property='pubs')

    with pytest.raises(AttributeError):
        remap_edge_property(graphs[0], edge_predicate='biolink:subclass_of', old_property='biolink:predicate', new_property='pubs')