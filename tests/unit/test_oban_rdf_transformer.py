import os
import pprint

from kgx import ObanRdfTransformer
from tests import print_graph

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')

cmap={
    'HGNC': 'https://www.genenames.org/data/gene-symbol-report/#!/hgnc_id/',
    'OMIM': 'http://omim.org/entry/'
}


def test_parse1():
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

    n1 = t1.graph.nodes()['HP:0000505']
    assert len(n1['biolink:category']) == 1
    assert 'biolink:NamedThing' in n1['biolink:category']

    e1 = list(t1.graph.get_edge('OMIM:166400', 'HP:0000006').values())[0]
    assert e1['biolink:subject'] == 'OMIM:166400'
    assert e1['biolink:object'] == 'HP:0000006'
    assert e1['biolink:relation'] == 'RO:0000091'
    assert e1['biolink:type'] == 'OBAN:association'
    assert e1['biolink:has_evidence'] == 'ECO:0000501'

    e2 = list(t1.graph.get_edge('ORPHA:93262', 'HP:0000505').values())[0]
    assert e2['biolink:subject'] == 'ORPHA:93262'
    assert e2['biolink:object'] == 'HP:0000505'
    assert e2['biolink:relation'] == 'RO:0002200'
    assert e2['biolink:type'] == 'OBAN:association'
    assert e2['MONARCH:frequencyOfPhenotype'] == 'HP:0040283'

def test_parse2():
    np = {
        'http://purl.obolibrary.org/obo/RO_0002558',
        'http://purl.org/dc/elements/1.1/source',
        'https://monarchinitiative.org/frequencyOfPhenotype'
    }
    predicate_mapping = {
        'https://monarchinitiative.org/frequencyOfPhenotype': ':frequency_of_phenotype'
    }
    t1 = ObanRdfTransformer(curie_map=cmap)
    t1.set_predicate_mapping(predicate_mapping)
    t1.parse(os.path.join(resource_dir, 'rdf', 'oban-test.nt'), input_format='nt', node_property_predicates=np)
    print_graph(t1.graph)
    assert t1.graph.number_of_nodes() == 14
    assert t1.graph.number_of_edges() == 7

    n1 = t1.graph.nodes()['HP:0000505']
    assert len(n1['biolink:category']) == 1
    assert 'biolink:NamedThing' in n1['biolink:category']

    e1 = list(t1.graph.get_edge('OMIM:166400', 'HP:0000006').values())[0]
    assert e1['biolink:subject'] == 'OMIM:166400'
    assert e1['biolink:object'] == 'HP:0000006'
    assert e1['biolink:relation'] == 'RO:0000091'
    assert e1['biolink:type'] == 'OBAN:association'
    assert e1['biolink:has_evidence'] == 'ECO:0000501'
    # assert e1['dc_source'] == 'OMIM:166400'

    e2 = list(t1.graph.get_edge('ORPHA:93262', 'HP:0000505').values())[0]
    assert e2['biolink:subject'] == 'ORPHA:93262'
    assert e2['biolink:object'] == 'HP:0000505'
    assert e2['biolink:relation'] == 'RO:0002200'
    assert e2['biolink:type'] == 'OBAN:association'
    assert e2[':frequency_of_phenotype'] == 'HP:0040283'
    # assert e2['dc_source'] == 'ORPHA:93262'

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

    n1 = t1.graph.nodes()['HP:0000505']
    assert len(n1['biolink:category']) == 1
    assert 'biolink:NamedThing' in n1['biolink:category']

    e1 = list(t1.graph.get_edge('OMIM:166400', 'HP:0000006').values())[0]
    assert e1['biolink:subject'] == 'OMIM:166400'
    assert e1['biolink:object'] == 'HP:0000006'
    assert e1['biolink:relation'] == 'RO:0000091'
    assert e1['biolink:type'] == 'OBAN:association'
    assert e1['biolink:has_evidence'] == 'ECO:0000501'

    e2 = list(t1.graph.get_edge('ORPHA:93262', 'HP:0000505').values())[0]
    assert e2['biolink:subject'] == 'ORPHA:93262'
    assert e2['biolink:object'] == 'HP:0000505'
    assert e2['biolink:relation'] == 'RO:0002200'
    assert e2['biolink:type'] == 'OBAN:association'
    assert e2['MONARCH:frequencyOfPhenotype'] == 'HP:0040283'

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
    predicate_mapping = {
        'https://monarchinitiative.org/frequencyOfPhenotype': ':frequency_of_phenotype'
    }
    t1 = ObanRdfTransformer(curie_map=cmap)
    t1.set_predicate_mapping(predicate_mapping)
    t1.parse(os.path.join(resource_dir, 'rdf', 'oban-test.nt'), input_format='nt', node_property_predicates=np)
    print_graph(t1.graph)
    assert t1.graph.number_of_nodes() == 14
    assert t1.graph.number_of_edges() == 7

    n1t1 = t1.graph.nodes()['HP:0000505']
    assert len(n1t1['biolink:category']) == 1
    assert 'biolink:NamedThing' in n1t1['biolink:category']

    e1t1 = list(t1.graph.get_edge('OMIM:166400', 'HP:0000006').values())[0]
    assert e1t1['biolink:subject'] == 'OMIM:166400'
    assert e1t1['biolink:object'] == 'HP:0000006'
    assert e1t1['biolink:relation'] == 'RO:0000091'
    assert e1t1['biolink:type'] == 'OBAN:association'
    assert e1t1['biolink:has_evidence'] == 'ECO:0000501'
    assert e1t1['biolink:source'] == 'OMIM:166400'

    e2t1 = list(t1.graph.get_edge('ORPHA:93262', 'HP:0000505').values())[0]
    assert e2t1['biolink:subject'] == 'ORPHA:93262'
    assert e2t1['biolink:object'] == 'HP:0000505'
    assert e2t1['biolink:relation'] == 'RO:0002200'
    assert e2t1['biolink:type'] == 'OBAN:association'
    assert e2t1[':frequency_of_phenotype'] == 'HP:0040283'
    assert e2t1['biolink:source'] == 'ORPHA:93262'

    t1.set_property_types({':frequency_of_phenotype': 'uriorcurie', 'source': 'uriorcurie'})
    t1.save(os.path.join(target_dir, 'oban-export.ttl'), output_format='ttl')
    t1.save(os.path.join(target_dir, 'oban-export.nt'), output_format='nt')

    t2 = ObanRdfTransformer(curie_map=cmap)
    t2.set_predicate_mapping(predicate_mapping)
    t2.parse(os.path.join(target_dir, 'oban-export.ttl'), input_format='ttl', node_property_predicates=np)
    print_graph(t2.graph)
    assert t2.graph.number_of_nodes() == 14
    assert t2.graph.number_of_edges() == 7

    n1t2 = t2.graph.nodes()['HP:0000505']
    assert len(n1t2['biolink:category']) == 1
    assert 'biolink:NamedThing' in n1t2['biolink:category']
    print(list(t2.graph.get_edge('OMIM:166400', 'HP:0000006').values()))
    e1t2 = list(t2.graph.get_edge('OMIM:166400', 'HP:0000006').values())[0]
    assert e1t2['biolink:subject'] == 'OMIM:166400'
    assert e1t2['biolink:object'] == 'HP:0000006'
    assert e1t2['biolink:relation'] == 'RO:0000091'
    assert 'biolink:Association' in e1t2['biolink:category']
    assert e1t2['biolink:has_evidence'] == 'ECO:0000501'
    assert e1t2['biolink:source'] == 'OMIM:166400'

    e2t2 = list(t2.graph.get_edge('ORPHA:93262', 'HP:0000505').values())[0]
    assert e2t2['biolink:subject'] == 'ORPHA:93262'
    assert e2t2['biolink:object'] == 'HP:0000505'
    assert e2t2['biolink:relation'] == 'RO:0002200'
    assert 'biolink:Association' in e2t2['biolink:category']
    assert e2t2[':frequency_of_phenotype'] == 'HP:0040283'
    assert e2t2['biolink:source'] == 'ORPHA:93262'

    t3 = ObanRdfTransformer(curie_map=cmap)
    t3.set_predicate_mapping(predicate_mapping)
    t3.parse(os.path.join(target_dir, 'oban-export.nt'), input_format='nt', node_property_predicates=np)
    assert t3.graph.number_of_nodes() == 14
    assert t3.graph.number_of_edges() == 7

    n1t3 = t1.graph.nodes()['HP:0000505']
    assert len(n1t3['biolink:category']) == 1
    assert 'biolink:NamedThing' in n1t3['biolink:category']

    e1t3 = list(t3.graph.get_edge('OMIM:166400', 'HP:0000006').values())[0]
    assert e1t3['biolink:subject'] == 'OMIM:166400'
    assert e1t3['biolink:object'] == 'HP:0000006'
    assert e1t3['biolink:relation'] == 'RO:0000091'
    assert 'biolink:Association' in e1t3['biolink:category']
    assert e1t3['biolink:has_evidence'] == 'ECO:0000501'
    assert e1t3['biolink:source'] == 'OMIM:166400'

    e2t3 = list(t3.graph.get_edge('ORPHA:93262', 'HP:0000505').values())[0]
    assert e2t3['biolink:subject'] == 'ORPHA:93262'
    assert e2t3['biolink:object'] == 'HP:0000505'
    assert e2t3['biolink:relation'] == 'RO:0002200'
    assert 'biolink:Association' in e2t3['biolink:category']
    assert e2t3[':frequency_of_phenotype'] == 'HP:0040283'
    assert e2t3['biolink:source'] == 'ORPHA:93262'


def test_save3():
    np = {
        'http://purl.obolibrary.org/obo/RO_0002558',
        'http://purl.org/dc/elements/1.1/source',
        'https://monarchinitiative.org/frequencyOfPhenotype'
    }
    predicate_mapping = {
        'https://monarchinitiative.org/frequencyOfPhenotype': ':frequency_of_phenotype'
    }
    prop_types = {
        'http://purl.obolibrary.org/obo/RO_0002558': 'uriorcurie',
        'https://monarchinitiative.org/frequencyOfPhenotype': 'uriorcurie'
    }
    t1 = ObanRdfTransformer(curie_map=cmap)
    t1.set_predicate_mapping(predicate_mapping)
    t1.set_property_types(prop_types)
    t1.parse(os.path.join(resource_dir, 'rdf', 'oban-test.nt'), input_format='nt', node_property_predicates=np)
    print_graph(t1.graph)
    assert t1.graph.number_of_nodes() == 14
    assert t1.graph.number_of_edges() == 7

    n1 = t1.graph.nodes()['HP:0000505']
    assert len(n1['biolink:category']) == 1
    assert 'biolink:NamedThing' in n1['biolink:category']

    e1 = list(t1.graph.get_edge('OMIM:166400', 'HP:0000006').values())[0]
    assert e1['biolink:subject'] == 'OMIM:166400'
    assert e1['biolink:object'] == 'HP:0000006'
    assert e1['biolink:relation'] == 'RO:0000091'
    assert e1['biolink:type'] == 'OBAN:association'
    assert e1['biolink:has_evidence'] == 'ECO:0000501'
    # assert e1['dc_source'] == 'OMIM:166400'

    e2 = list(t1.graph.get_edge('ORPHA:93262', 'HP:0000505').values())[0]
    assert e2['biolink:subject'] == 'ORPHA:93262'
    assert e2['biolink:object'] == 'HP:0000505'
    assert e2['biolink:relation'] == 'RO:0002200'
    assert e2['biolink:type'] == 'OBAN:association'
    assert e2[':frequency_of_phenotype'] == 'HP:0040283'
    # assert e2['dc_source'] == 'ORPHA:93262'

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
