import os
import pprint

from kgx import ObanRdfTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')

cmap={
    'HGNC': 'https://www.genenames.org/data/gene-symbol-report/#!/hgnc_id/',
    'OMIM': 'http://omim.org/entry/'
}

def print_graph(g):
    pprint.pprint([x for x in g.nodes(data=True)])
    pprint.pprint([x for x in g.edges(data=True)])


def test_parse():
    np = {
        'http://purl.obolibrary.org/obo/RO_0002558',
        'http://purl.org/dc/elements/1.1/source',
        'https://monarchinitiative.org/frequencyOfPhenotype'
    }
    t1 = ObanRdfTransformer(curie_map=cmap)
    t1.parse(os.path.join(resource_dir, 'rdf', 'oban-test.nt'), input_format='nt', node_property_predicates=np)
    print_graph(t1.graph)
    assert t1.graph.number_of_nodes() == 14
    assert t1.graph.number_of_edges() == 7

    n1 = t1.graph.nodes['HP:0000505']
    assert len(n1['category']) == 1
    assert 'biolink:NamedThing' in n1['category']

    e1 = list(t1.graph.get_edge_data('OMIM:166400', 'HP:0000006').values())[0]
    assert e1['subject'] == 'OMIM:166400'
    assert e1['object'] == 'HP:0000006'
    assert e1['relation'] == 'RO:0000091'
    assert e1['type'] == 'OBAN:association'
    assert e1['has_evidence'] == 'ECO:0000501'

    e2 = list(t1.graph.get_edge_data('Orphanet:93262', 'HP:0000505').values())[0]
    assert e2['subject'] == 'Orphanet:93262'
    assert e2['object'] == 'HP:0000505'
    assert e2['relation'] == 'RO:0002200'
    assert e2['type'] == 'OBAN:association'
    assert e2['frequencyOfPhenotype'] == 'HP:0040283'


def test_save1():
    np = {
        'http://purl.obolibrary.org/obo/RO_0002558',
        'http://purl.org/dc/elements/1.1/source',
        'https://monarchinitiative.org/frequencyOfPhenotype'
    }
    t1 = ObanRdfTransformer(curie_map=cmap)
    t1.parse(os.path.join(resource_dir, 'rdf', 'oban-test.nt'), input_format='nt', node_property_predicates=np)
    print_graph(t1.graph)
    assert t1.graph.number_of_nodes() == 14
    assert t1.graph.number_of_edges() == 7

    n1 = t1.graph.nodes['HP:0000505']
    assert len(n1['category']) == 1
    assert 'biolink:NamedThing' in n1['category']

    e1 = list(t1.graph.get_edge_data('OMIM:166400', 'HP:0000006').values())[0]
    assert e1['subject'] == 'OMIM:166400'
    assert e1['object'] == 'HP:0000006'
    assert e1['relation'] == 'RO:0000091'
    assert e1['type'] == 'OBAN:association'
    assert e1['has_evidence'] == 'ECO:0000501'

    e2 = list(t1.graph.get_edge_data('Orphanet:93262', 'HP:0000505').values())[0]
    assert e2['subject'] == 'Orphanet:93262'
    assert e2['object'] == 'HP:0000505'
    assert e2['relation'] == 'RO:0002200'
    assert e2['type'] == 'OBAN:association'
    assert e2['frequencyOfPhenotype'] == 'HP:0040283'

    t1.save(os.path.join(target_dir, 'oban-export.ttl'), output_format='ttl')
    t1.save(os.path.join(target_dir, 'oban-export.nt'), output_format='nt')

    t2 = ObanRdfTransformer(curie_map=cmap)
    t2.parse(os.path.join(target_dir, 'oban-export.ttl'), input_format='ttl', node_property_predicates=np)
    assert t2.graph.number_of_nodes() == 14
    assert t2.graph.number_of_edges() == 7

    t3 = ObanRdfTransformer(curie_map=cmap)
    t3.parse(os.path.join(target_dir, 'oban-export.nt'), input_format='nt', node_property_predicates=np)
    assert t3.graph.number_of_nodes() == 14
    assert t3.graph.number_of_edges() == 7

def test_save2():
    np = {
        'http://purl.obolibrary.org/obo/RO_0002558',
        'http://purl.org/dc/elements/1.1/source',
        'https://monarchinitiative.org/frequencyOfPhenotype'
    }
    prop_types = {
        'http://purl.obolibrary.org/obo/RO_0002558': 'uriorcurie',
        'https://monarchinitiative.org/frequencyOfPhenotype': 'uriorcurie'
    }
    t1 = ObanRdfTransformer(curie_map=cmap)
    t1.set_property_types(prop_types)
    t1.parse(os.path.join(resource_dir, 'rdf', 'oban-test.nt'), input_format='nt', node_property_predicates=np)
    print_graph(t1.graph)
    assert t1.graph.number_of_nodes() == 14
    assert t1.graph.number_of_edges() == 7

    n1 = t1.graph.nodes['HP:0000505']
    assert len(n1['category']) == 1
    assert 'biolink:NamedThing' in n1['category']

    e1 = list(t1.graph.get_edge_data('OMIM:166400', 'HP:0000006').values())[0]
    assert e1['subject'] == 'OMIM:166400'
    assert e1['object'] == 'HP:0000006'
    assert e1['relation'] == 'RO:0000091'
    assert e1['type'] == 'OBAN:association'
    assert e1['has_evidence'] == 'ECO:0000501'

    e2 = list(t1.graph.get_edge_data('Orphanet:93262', 'HP:0000505').values())[0]
    assert e2['subject'] == 'Orphanet:93262'
    assert e2['object'] == 'HP:0000505'
    assert e2['relation'] == 'RO:0002200'
    assert e2['type'] == 'OBAN:association'
    assert e2['frequencyOfPhenotype'] == 'HP:0040283'

    t1.save(os.path.join(target_dir, 'oban-export.ttl'), output_format='ttl')
    t1.save(os.path.join(target_dir, 'oban-export.nt'), output_format='nt')

    t2 = ObanRdfTransformer(curie_map=cmap)
    t2.parse(os.path.join(target_dir, 'oban-export.ttl'), input_format='ttl', node_property_predicates=np)
    assert t2.graph.number_of_nodes() == 14
    assert t2.graph.number_of_edges() == 7

    t3 = ObanRdfTransformer(curie_map=cmap)
    t3.parse(os.path.join(target_dir, 'oban-export.nt'), input_format='nt', node_property_predicates=np)
    assert t3.graph.number_of_nodes() == 14
    assert t3.graph.number_of_edges() == 7