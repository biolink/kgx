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

def test_parse2():
    np = {
        'http://purl.obolibrary.org/obo/RO_0002558',
        'http://purl.org/dc/elements/1.1/source',
        'https://monarchinitiative.org/frequencyOfPhenotype'
    }
    predicate_mapping = {
        'http://purl.org/dc/elements/1.1/source': 'dc_source',
        'https://monarchinitiative.org/frequencyOfPhenotype': 'frequency_of_phenotype'
    }
    t1 = ObanRdfTransformer(curie_map=cmap)
    t1.set_predicate_mapping(predicate_mapping)
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
    assert e1['dc_source'] == 'OMIM:166400'

    e2 = list(t1.graph.get_edge_data('Orphanet:93262', 'HP:0000505').values())[0]
    assert e2['subject'] == 'Orphanet:93262'
    assert e2['object'] == 'HP:0000505'
    assert e2['relation'] == 'RO:0002200'
    assert e2['type'] == 'OBAN:association'
    assert e2['frequency_of_phenotype'] == 'HP:0040283'
    assert e2['dc_source'] == 'Orphanet:93262'

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
    predicate_mapping = {
        'http://purl.org/dc/elements/1.1/source': 'dc_source',
        'https://monarchinitiative.org/frequencyOfPhenotype': 'frequency_of_phenotype'
    }
    t1 = ObanRdfTransformer(curie_map=cmap)
    t1.set_predicate_mapping(predicate_mapping)
    t1.parse(os.path.join(resource_dir, 'rdf', 'oban-test.nt'), input_format='nt', node_property_predicates=np)
    print_graph(t1.graph)
    assert t1.graph.number_of_nodes() == 14
    assert t1.graph.number_of_edges() == 7

    n1t1 = t1.graph.nodes['HP:0000505']
    assert len(n1t1['category']) == 1
    assert 'biolink:NamedThing' in n1t1['category']

    e1t1 = list(t1.graph.get_edge_data('OMIM:166400', 'HP:0000006').values())[0]
    assert e1t1['subject'] == 'OMIM:166400'
    assert e1t1['object'] == 'HP:0000006'
    assert e1t1['relation'] == 'RO:0000091'
    assert e1t1['type'] == 'OBAN:association'
    assert e1t1['has_evidence'] == 'ECO:0000501'
    assert e1t1['dc_source'] == 'OMIM:166400'

    e2t1 = list(t1.graph.get_edge_data('Orphanet:93262', 'HP:0000505').values())[0]
    assert e2t1['subject'] == 'Orphanet:93262'
    assert e2t1['object'] == 'HP:0000505'
    assert e2t1['relation'] == 'RO:0002200'
    assert e2t1['type'] == 'OBAN:association'
    assert e2t1['frequency_of_phenotype'] == 'HP:0040283'
    assert e2t1['dc_source'] == 'Orphanet:93262'

    t1.set_property_types({'frequency_of_phenotype': 'uriorcurie', 'dc_source': 'uriorcurie'})
    t1.save(os.path.join(target_dir, 'oban-export.ttl'), output_format='ttl')
    t1.save(os.path.join(target_dir, 'oban-export.nt'), output_format='nt')

    t2 = ObanRdfTransformer(curie_map=cmap)
    t2.set_predicate_mapping(predicate_mapping)
    t2.parse(os.path.join(target_dir, 'oban-export.ttl'), input_format='ttl', node_property_predicates=np)
    print_graph(t2.graph)
    assert t2.graph.number_of_nodes() == 14
    assert t2.graph.number_of_edges() == 7

    n1t2 = t2.graph.nodes['HP:0000505']
    assert len(n1t2['category']) == 1
    assert 'biolink:NamedThing' in n1t2['category']

    e1t2 = list(t2.graph.get_edge_data('OMIM:166400', 'HP:0000006').values())[0]
    assert e1t2['subject'] == 'OMIM:166400'
    assert e1t2['object'] == 'HP:0000006'
    assert e1t2['relation'] == 'RO:0000091'
    assert e1t2['type'] == 'biolink:Association'
    assert e1t2['has_evidence'] == 'ECO:0000501'
    assert e1t2['dc_source'] == 'OMIM:166400'

    e2t2 = list(t2.graph.get_edge_data('Orphanet:93262', 'HP:0000505').values())[0]
    assert e2t2['subject'] == 'Orphanet:93262'
    assert e2t2['object'] == 'HP:0000505'
    assert e2t2['relation'] == 'RO:0002200'
    assert e2t2['type'] == 'biolink:Association'
    assert e2t2['frequency_of_phenotype'] == 'HP:0040283'
    assert e2t2['dc_source'] == 'Orphanet:93262'

    t3 = ObanRdfTransformer(curie_map=cmap)
    t3.set_predicate_mapping(predicate_mapping)
    t3.parse(os.path.join(target_dir, 'oban-export.nt'), input_format='nt', node_property_predicates=np)
    assert t3.graph.number_of_nodes() == 14
    assert t3.graph.number_of_edges() == 7

    n1t3 = t1.graph.nodes['HP:0000505']
    assert len(n1t3['category']) == 1
    assert 'biolink:NamedThing' in n1t3['category']

    e1t3 = list(t3.graph.get_edge_data('OMIM:166400', 'HP:0000006').values())[0]
    assert e1t3['subject'] == 'OMIM:166400'
    assert e1t3['object'] == 'HP:0000006'
    assert e1t3['relation'] == 'RO:0000091'
    assert e1t3['type'] == 'biolink:Association'
    assert e1t3['has_evidence'] == 'ECO:0000501'
    assert e1t3['dc_source'] == 'OMIM:166400'

    e2t3 = list(t3.graph.get_edge_data('Orphanet:93262', 'HP:0000505').values())[0]
    assert e2t3['subject'] == 'Orphanet:93262'
    assert e2t3['object'] == 'HP:0000505'
    assert e2t3['relation'] == 'RO:0002200'
    assert e2t3['type'] == 'biolink:Association'
    assert e2t3['frequency_of_phenotype'] == 'HP:0040283'
    assert e2t3['dc_source'] == 'Orphanet:93262'


def test_save3():
    np = {
        'http://purl.obolibrary.org/obo/RO_0002558',
        'http://purl.org/dc/elements/1.1/source',
        'https://monarchinitiative.org/frequencyOfPhenotype'
    }
    predicate_mapping = {
        'http://purl.org/dc/elements/1.1/source': 'dc_source',
        'https://monarchinitiative.org/frequencyOfPhenotype': 'frequency_of_phenotype'
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

    n1 = t1.graph.nodes['HP:0000505']
    assert len(n1['category']) == 1
    assert 'biolink:NamedThing' in n1['category']

    e1 = list(t1.graph.get_edge_data('OMIM:166400', 'HP:0000006').values())[0]
    assert e1['subject'] == 'OMIM:166400'
    assert e1['object'] == 'HP:0000006'
    assert e1['relation'] == 'RO:0000091'
    assert e1['type'] == 'OBAN:association'
    assert e1['has_evidence'] == 'ECO:0000501'
    assert e1['dc_source'] == 'OMIM:166400'

    e2 = list(t1.graph.get_edge_data('Orphanet:93262', 'HP:0000505').values())[0]
    assert e2['subject'] == 'Orphanet:93262'
    assert e2['object'] == 'HP:0000505'
    assert e2['relation'] == 'RO:0002200'
    assert e2['type'] == 'OBAN:association'
    assert e2['frequency_of_phenotype'] == 'HP:0040283'
    assert e2['dc_source'] == 'Orphanet:93262'

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
