import os

import pytest
import pprint

import rdflib
from rdflib import URIRef

from kgx import RdfTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')


def print_graph(g):
    pprint.pprint([x for x in g.nodes(data=True)])
    pprint.pprint([x for x in g.edges(data=True)])


def test_parse1():
    t1 = RdfTransformer()
    t1.parse(os.path.join(resource_dir, 'rdf', 'test1.nt'))
    print_graph(t1.graph)
    assert t1.graph.number_of_nodes() == 2
    assert t1.graph.number_of_edges() == 1

    n1 = t1.graph.nodes['ENSEMBL:ENSG0000000000001']
    assert n1['type'] == 'SO:0000704'
    assert len(n1['category']) == 4
    assert 'biolink:Gene' in n1['category']
    assert 'biolink:GenomicEntity' in n1['category']
    assert 'biolink:NamedThing' in n1['category']
    assert n1['name'] == 'Test Gene 123'
    assert n1['description'] == 'This is a Test Gene 123'
    assert 'Test Dataset' in n1['provided_by']

    n2 = t1.graph.nodes['ENSEMBL:ENSG0000000000002']
    assert n2['type'] == 'SO:0000704'
    assert len(n2['category']) == 4
    assert 'biolink:Gene' in n2['category']
    assert 'biolink:GenomicEntity' in n2['category']
    assert 'biolink:NamedThing' in n1['category']
    assert n2['name'] == 'Test Gene 456'
    assert n2['description'] == 'This is a Test Gene 456'
    assert 'Test Dataset' in n2['provided_by']

    [(u, v, data)] = t1.graph.edges(data=True)
    assert u == data['subject'] == 'ENSEMBL:ENSG0000000000001'
    assert v == data['object'] == 'ENSEMBL:ENSG0000000000002'
    assert data['edge_label'] == 'biolink:interacts_with'
    assert data['relation'] == 'biolink:interacts_with'


def test_parse2():
    np = {f"https://www.example.org/UNKNOWN/{x}" for x in ['fusion', 'homology', 'combined_score', 'cooccurence']}
    t1 = RdfTransformer()
    t1.parse(os.path.join(resource_dir, 'rdf', 'test2.nt'), node_property_predicates=np)
    print_graph(t1.graph)
    assert t1.graph.number_of_nodes() == 4
    assert t1.graph.number_of_edges() == 3

    n1 = t1.graph.nodes['ENSEMBL:ENSG0000000000001']
    assert n1['type'] == 'SO:0000704'
    assert len(n1['category']) == 4
    assert 'biolink:Gene' in n1['category']
    assert 'biolink:GenomicEntity' in n1['category']
    assert 'biolink:NamedThing' in n1['category']
    assert n1['name'] == 'Test Gene 123'
    assert n1['description'] == 'This is a Test Gene 123'
    assert 'Test Dataset' in n1['provided_by']

    n2 = t1.graph.nodes['ENSEMBL:ENSG0000000000002']
    assert n2['type'] == 'SO:0000704'
    assert len(n2['category']) == 4
    assert 'biolink:Gene' in n2['category']
    assert 'biolink:GenomicEntity' in n2['category']
    assert 'biolink:NamedThing' in n1['category']
    assert n2['name'] == 'Test Gene 456'
    assert n2['description'] == 'This is a Test Gene 456'
    assert 'Test Dataset' in n2['provided_by']

    print(t1.graph.get_edge_data('ENSEMBL:ENSP0000000000001', 'ENSEMBL:ENSP0000000000002'))
    e1 = list(t1.graph.get_edge_data('ENSEMBL:ENSP0000000000001', 'ENSEMBL:ENSP0000000000002').values())[0]
    assert e1['subject'] == 'ENSEMBL:ENSP0000000000001'
    assert e1['object'] == 'ENSEMBL:ENSP0000000000002'
    assert e1['edge_label'] == 'biolink:interacts_with'
    assert e1['relation'] == 'biolink:interacts_with'
    assert e1['type'] == 'biolink:Association'
    assert e1['id'] == 'urn:uuid:fcf76807-f909-4ccb-b40a-3b79b49aa518'
    assert e1['fusion'] == '0'
    assert e1['homology'] == '0.0'
    assert e1['combined_score'] == '490.0'
    assert e1['cooccurence'] == '332'


def test_parse3():
    np = {f"https://www.example.org/UNKNOWN/{x}" for x in ['fusion', 'homology', 'combined_score', 'cooccurence']}
    t1 = RdfTransformer()
    t1.parse(os.path.join(resource_dir, 'rdf', 'test3.nt'), node_property_predicates=np)
    print_graph(t1.graph)
    assert t1.graph.number_of_nodes() == 7
    assert t1.graph.number_of_edges() == 6

    n1 = t1.graph.nodes['ENSEMBL:ENSG0000000000001']
    assert n1['type'] == 'SO:0000704'
    assert len(n1['category']) == 4
    assert 'biolink:Gene' in n1['category']
    assert 'biolink:GenomicEntity' in n1['category']
    assert 'biolink:NamedThing' in n1['category']
    assert n1['name'] == 'Test Gene 123'
    assert n1['description'] == 'This is a Test Gene 123'
    assert 'Test Dataset' in n1['provided_by']

    n2 = t1.graph.nodes['ENSEMBL:ENSG0000000000002']
    assert n2['type'] == 'SO:0000704'
    assert len(n2['category']) == 4
    assert 'biolink:Gene' in n2['category']
    assert 'biolink:GenomicEntity' in n2['category']
    assert 'biolink:NamedThing' in n1['category']
    assert n2['name'] == 'Test Gene 456'
    assert n2['description'] == 'This is a Test Gene 456'
    assert 'Test Dataset' in n2['provided_by']

    e1 = list(t1.graph.get_edge_data('ENSEMBL:ENSP0000000000001', 'ENSEMBL:ENSP0000000000002').values())[0]
    assert e1['subject'] == 'ENSEMBL:ENSP0000000000001'
    assert e1['object'] == 'ENSEMBL:ENSP0000000000002'
    assert e1['edge_label'] == 'biolink:interacts_with'
    assert e1['relation'] == 'biolink:interacts_with'
    assert e1['type'] == 'biolink:Association'
    assert e1['id'] == 'urn:uuid:fcf76807-f909-4ccb-b40a-3b79b49aa518'
    assert e1['fusion'] == '0'
    assert e1['homology'] == '0.0'
    assert e1['combined_score'] == '490.0'
    assert e1['cooccurence'] == '332'

    e2 = list(t1.graph.get_edge_data('ENSEMBL:ENSP0000000000001', 'UniProtKB:X0000001').values())[0]
    assert e2['subject'] == 'ENSEMBL:ENSP0000000000001'
    assert e2['object'] == 'UniProtKB:X0000001'
    assert e2['edge_label'] == 'biolink:same_as'
    assert e2['relation'] == 'owl:equivalentClass'

    e3 = list(t1.graph.get_edge_data('ENSEMBL:ENSP0000000000001', 'MONDO:0000001').values())[0]
    assert e3['subject'] == 'ENSEMBL:ENSP0000000000001'
    assert e3['object'] == 'MONDO:0000001'
    assert e3['edge_label'] == 'biolink:treats'
    assert e3['relation'] == 'RO:0002606'


def test_save1():
    t1 = RdfTransformer()
    t1.parse(os.path.join(resource_dir, 'rdf', 'test1.nt'))
    assert t1.graph.number_of_nodes() == 2
    assert t1.graph.number_of_edges() == 1

    t1.save(os.path.join(target_dir, 'test1-export.ttl'), output_format='turtle')
    t1.save(os.path.join(target_dir, 'test1-export.nt'), output_format='nt')

    t2 = RdfTransformer()
    t2.parse(os.path.join(target_dir, 'test1-export.ttl'))
    assert t2.graph.number_of_nodes() == 2
    assert t2.graph.number_of_edges() == 1

    t3 = RdfTransformer()
    t3.parse(os.path.join(target_dir, 'test1-export.nt'))
    assert t3.graph.number_of_nodes() == 2
    assert t3.graph.number_of_edges() == 1

    n1t1 = t1.graph.nodes['ENSEMBL:ENSG0000000000001']
    n1t2 = t2.graph.nodes['ENSEMBL:ENSG0000000000001']
    n1t3 = t3.graph.nodes['ENSEMBL:ENSG0000000000001']

    assert n1t1['type'] == n1t2['type'] == n1t3['type'] == 'SO:0000704'
    assert len(n1t1['category']) == len(n1t2['category']) == len(n1t3['category']) == 4
    assert 'biolink:Gene' in n1t1['category'] and 'biolink:Gene' in n1t2['category'] and 'biolink:Gene' in n1t3['category']
    assert 'biolink:GenomicEntity' in n1t1['category'] and 'biolink:GenomicEntity' in n1t2['category'] and 'biolink:GenomicEntity' in n1t3['category']
    assert 'biolink:NamedThing' in n1t1['category'] and 'biolink:NamedThing' in n1t2['category'] and 'biolink:NamedThing' in n1t3['category']
    assert n1t1['name'] == n1t2['name'] == n1t3['name'] == 'Test Gene 123'
    assert n1t1['description'] == n1t2['description'] == n1t3['description'] == 'This is a Test Gene 123'
    assert 'Test Dataset' in n1t1['provided_by'] and 'Test Dataset' in n1t2['provided_by'] and 'Test Dataset' in n1t3['provided_by']


def test_save2():
    np = {f"https://www.example.org/UNKNOWN/{x}" for x in ['fusion', 'homology', 'combined_score', 'cooccurence']}
    t1 = RdfTransformer()
    t1.parse(os.path.join(resource_dir, 'rdf', 'test2.nt'), node_property_predicates=np)
    print_graph(t1.graph)
    assert t1.graph.number_of_nodes() == 4
    assert t1.graph.number_of_edges() == 3
    t1.save(os.path.join(target_dir, 'test2-export.ttl'), output_format='turtle')
    t1.save(os.path.join(target_dir, 'test2-export.nt'), output_format='nt')

    t2 = RdfTransformer()
    t2.parse(os.path.join(target_dir, 'test2-export.ttl'), node_property_predicates=np)
    print_graph(t2.graph)
    assert t2.graph.number_of_nodes() == 4
    assert t2.graph.number_of_edges() == 3

    t3 = RdfTransformer()
    t3.parse(os.path.join(target_dir, 'test2-export.nt'), node_property_predicates=np)
    assert t3.graph.number_of_nodes() == 4
    assert t3.graph.number_of_edges() == 3

    n1t1 = t1.graph.nodes['ENSEMBL:ENSG0000000000001']
    n1t2 = t2.graph.nodes['ENSEMBL:ENSG0000000000001']
    n1t3 = t3.graph.nodes['ENSEMBL:ENSG0000000000001']

    assert n1t1['type'] == n1t2['type'] == n1t3['type'] == 'SO:0000704'
    assert len(n1t1['category']) == len(n1t2['category']) == len(n1t3['category']) == 4
    assert 'biolink:Gene' in n1t1['category'] and 'biolink:Gene' in n1t2['category'] and 'biolink:Gene' in n1t3['category']
    assert 'biolink:GenomicEntity' in n1t1['category'] and 'biolink:GenomicEntity' in n1t2['category'] and 'biolink:GenomicEntity' in n1t3['category']
    assert 'biolink:NamedThing' in n1t1['category'] and 'biolink:NamedThing' in n1t2['category'] and 'biolink:NamedThing' in n1t3['category']
    assert n1t1['name'] == n1t2['name'] == n1t3['name'] == 'Test Gene 123'
    assert n1t1['description'] == n1t2['description'] == n1t3['description'] == 'This is a Test Gene 123'
    assert 'Test Dataset' in n1t1['provided_by'] and 'Test Dataset' in n1t2['provided_by'] and 'Test Dataset' in n1t3['provided_by']

    e1t1 = list(t1.graph.get_edge_data('ENSEMBL:ENSP0000000000001', 'ENSEMBL:ENSP0000000000002').values())[0]
    e1t2 = list(t2.graph.get_edge_data('ENSEMBL:ENSP0000000000001', 'ENSEMBL:ENSP0000000000002').values())[0]
    e1t3 = list(t3.graph.get_edge_data('ENSEMBL:ENSP0000000000001', 'ENSEMBL:ENSP0000000000002').values())[0]

    assert e1t1['subject'] == e1t2['subject'] == e1t3['subject'] == 'ENSEMBL:ENSP0000000000001'
    assert e1t1['object'] == e1t2['object'] == e1t3['object'] == 'ENSEMBL:ENSP0000000000002'
    assert e1t1['edge_label'] == e1t2['edge_label'] == e1t3['edge_label'] == 'biolink:interacts_with'
    assert e1t1['relation'] == e1t2['relation'] == e1t3['relation'] == 'biolink:interacts_with'
    assert e1t1['type'] == e1t2['type'] == e1t3['type'] == 'biolink:Association'
    assert e1t1['id'] == e1t2['id'] == e1t3['id'] == 'urn:uuid:fcf76807-f909-4ccb-b40a-3b79b49aa518'
    assert e1t1['fusion'] == e1t2['fusion'] == e1t3['fusion'] == '0'
    assert e1t1['homology'] == e1t2['homology'] == e1t3['homology'] == '0.0'
    assert e1t1['combined_score'] == e1t2['combined_score'] == e1t3['combined_score'] == '490.0'
    assert e1t1['cooccurence'] == e1t2['cooccurence'] == e1t3['cooccurence'] == '332'


def test_save3():
    np = {f"https://www.example.org/UNKNOWN/{x}" for x in ['fusion', 'homology', 'combined_score', 'cooccurence']}
    t1 = RdfTransformer()
    t1.parse(os.path.join(resource_dir, 'rdf', 'test3.nt'), node_property_predicates=np)
    assert t1.graph.number_of_nodes() == 7
    assert t1.graph.number_of_edges() == 6
    t1.save(os.path.join(target_dir, 'test3-export.ttl'), output_format='turtle')
    t1.save(os.path.join(target_dir, 'test3-export.nt'), output_format='nt')

    t2 = RdfTransformer()
    t2.parse(os.path.join(target_dir, 'test3-export.ttl'), node_property_predicates=np)
    assert t2.graph.number_of_nodes() == 7
    assert t2.graph.number_of_edges() == 6

    t3 = RdfTransformer()
    t3.parse(os.path.join(target_dir, 'test3-export.nt'), node_property_predicates=np)
    assert t3.graph.number_of_nodes() == 7
    assert t3.graph.number_of_edges() == 6

    n1t1 = t1.graph.nodes['ENSEMBL:ENSG0000000000001']
    n1t2 = t2.graph.nodes['ENSEMBL:ENSG0000000000001']
    n1t3 = t3.graph.nodes['ENSEMBL:ENSG0000000000001']

    assert n1t1['type'] == n1t2['type'] == n1t3['type'] == 'SO:0000704'
    assert len(n1t1['category']) == len(n1t2['category']) == len(n1t3['category']) == 4
    assert 'biolink:Gene' in n1t1['category'] and 'biolink:Gene' in n1t2['category'] and 'biolink:Gene' in n1t3['category']
    assert 'biolink:GenomicEntity' in n1t1['category'] and 'biolink:GenomicEntity' in n1t2['category'] and 'biolink:GenomicEntity' in n1t3['category']
    assert 'biolink:NamedThing' in n1t1['category'] and 'biolink:NamedThing' in n1t2['category'] and 'biolink:NamedThing' in n1t3['category']
    assert n1t1['name'] == n1t2['name'] == n1t3['name'] == 'Test Gene 123'
    assert n1t1['description'] == n1t2['description'] == n1t3['description'] == 'This is a Test Gene 123'
    assert 'Test Dataset' in n1t1['provided_by'] and 'Test Dataset' in n1t2['provided_by'] and 'Test Dataset' in n1t3['provided_by']

    e1t1 = list(t1.graph.get_edge_data('ENSEMBL:ENSP0000000000001', 'ENSEMBL:ENSP0000000000002').values())[0]
    e1t2 = list(t2.graph.get_edge_data('ENSEMBL:ENSP0000000000001', 'ENSEMBL:ENSP0000000000002').values())[0]
    e1t3 = list(t3.graph.get_edge_data('ENSEMBL:ENSP0000000000001', 'ENSEMBL:ENSP0000000000002').values())[0]

    assert e1t1['subject'] == e1t2['subject'] == e1t3['subject'] == 'ENSEMBL:ENSP0000000000001'
    assert e1t1['object'] == e1t2['object'] == e1t3['object'] == 'ENSEMBL:ENSP0000000000002'
    assert e1t1['edge_label'] == e1t2['edge_label'] == e1t3['edge_label'] == 'biolink:interacts_with'
    assert e1t1['relation'] == e1t2['relation'] == e1t3['relation'] == 'biolink:interacts_with'
    assert e1t1['type'] == e1t2['type'] == e1t3['type'] == 'biolink:Association'
    assert e1t1['id'] == e1t2['id'] == e1t3['id'] == 'urn:uuid:fcf76807-f909-4ccb-b40a-3b79b49aa518'
    assert e1t1['fusion'] == e1t2['fusion'] == e1t3['fusion'] == '0'
    assert e1t1['homology'] == e1t2['homology'] == e1t3['homology'] == '0.0'
    assert e1t1['combined_score'] == e1t2['combined_score'] == e1t3['combined_score'] == '490.0'
    assert e1t1['cooccurence'] == e1t2['cooccurence'] == e1t3['cooccurence'] == '332'


@pytest.mark.parametrize("query", [
    (
        {'id': 'ABC:123', 'category': 'biolink:NamedThing', 'prop1': [1, 2, 3]},
        {'category': ['biolink:NamedThing', 'biolink:Gene'], 'prop1': [4]},
        {'category': ['biolink:NamedThing', 'biolink:Gene']},
        {'prop1': [1, 2, 3, 4]}
    ),
    (
        {'id': 'ABC:123', 'category': ['biolink:NamedThing'], 'prop1': 1},
        {'category': {'biolink:NamedThing', 'biolink:Gene'}, 'prop1': [2, 3]},
        {'category': ['biolink:NamedThing', 'biolink:Gene']},
        {'prop1': [1, 2, 3]}
    ),
    (
        {'id': 'ABC:123', 'category': ['biolink:NamedThing'], 'provided_by': 'test'},
        {'id': 'DEF:456', 'category': ('biolink:NamedThing', 'biolink:Gene'), 'provided_by': 'test'},
        {'category': ['biolink:NamedThing', 'biolink:Gene']},
        {'provided_by': ['test']}
    ),
    (
        {'subject': 'Orphanet:331206', 'object': 'HP:0004429', 'relation': 'RO:0002200', 'edge_label': 'biolink:has_phenotype', 'id': 'bfada868a8309f2b7849', 'type': 'OBAN:association'},
        {'subject': 'Orphanet:331206', 'object': 'HP:0004429', 'relation': 'RO:0002200', 'edge_label': 'biolink:has_phenotype', 'id': 'bfada868a8309f2b7849', 'type': 'OBAN:association'},
        {},
        {}
    ),
    (
        {'subject': 'Orphanet:331206', 'object': 'HP:0004429', 'relation': 'RO:0002200',
         'edge_label': 'biolink:has_phenotype', 'id': 'bfada868a8309f2b7849', 'type': 'OBAN:association', 'source': 'Orphanet:331206'},
        {'subject': 'Orphanet:331206', 'object': 'HP:0004429', 'relation': 'RO:0002200',
         'edge_label': 'biolink:has_phenotype', 'id': 'bfada868a8309f2b7849', 'type': 'OBAN:association', 'source': 'Orphanet:331206'},
        {},
        {'source': ['Orphanet:331206', 'Orphanet:331206']}
    )
])
def test_prepare_data_dict(query):
    rt = RdfTransformer()
    new_data = rt._prepare_data_dict(query[0], query[1])
    for k, v in query[2].items():
        assert new_data[k] == v
    for k, v in query[3].items():
        assert new_data[k] == v


@pytest.mark.parametrize("query", [
    ('id', 'uriorcurie', 'MONDO:000001', 'URIRef', None),
    ('name', 'xsd:string', 'Test concept name', 'Literal', rdflib.term.URIRef('http://www.w3.org/2001/XMLSchema#string')),
    ('edge_label', 'uriorcurie', 'biolink:related_to', 'URIRef', None),
    ('relation', 'uriorcurie', 'RO:000000', 'URIRef', None),
    ('custom_property1', 'uriorcurie', 'X:123', 'URIRef', None),
    ('custom_property2', 'xsd:float', '480.213', 'Literal', rdflib.term.URIRef('http://www.w3.org/2001/XMLSchema#float')),
])
def test_prepare_object(query):
    rt = RdfTransformer()
    o = rt._prepare_object(query[0], query[1], query[2])
    assert type(o).__name__ == query[3]
    if query[4]:
        assert o.datatype == query[4]


@pytest.mark.parametrize("query", [
    ('name', 'xsd:string'),
    ('edge_label', 'uriorcurie'),
    ('xyz', 'xsd:string')
])
def test_get_property_type(query):
    rt = RdfTransformer()
    assert rt._get_property_type(query[0]) == query[1]


@pytest.mark.parametrize("query", [
    ('MONDO:000001', 'URIRef', 'http://purl.obolibrary.org/obo/MONDO_000001'),
    ('urn:uuid:12345', 'URIRef', 'urn:uuid:12345'),
    (':new_prop', 'URIRef', 'https://www.example.org/UNKNOWN/new_prop'),
    ('edge_label', 'URIRef', 'https://w3id.org/biolink/vocab/edge_label'),
    ('type', 'URIRef', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
])
def test_uriref(query):
    rt = RdfTransformer()
    x = rt.uriref(query[0])
    assert type(x).__name__ == query[1]
    assert str(x) == query[2]

@pytest.mark.parametrize("query", [
    ('http://purl.org/oban/association_has_object', 'biolink:object', 'OBAN:association_has_object', 'association_has_object'),
    ('http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'biolink:type', 'rdf:type', 'type'),
    ('https://monarchinitiative.org/frequencyOfPhenotype', None, 'MONARCH:frequencyOfPhenotype', 'frequencyOfPhenotype'),
    ('http://purl.obolibrary.org/obo/RO_0002200', 'biolink:has_phenotype', 'RO:0002200', '0002200'),
    ('http://www.w3.org/2002/07/owl#equivalentClass', 'biolink:same_as', 'owl:equivalentClass', 'equivalentClass'),
    ('https://www.example.org/UNKNOWN/new_prop', None, ':new_prop', 'new_prop')
])
def test_process_predicate(query):
    rt = RdfTransformer()
    x = rt.process_predicate(query[0])
    assert x[0] == query[1]
    assert x[1] == query[2]
    assert x[2] == query[3]

