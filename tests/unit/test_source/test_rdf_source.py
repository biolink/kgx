import os

from kgx.source import RdfSource
from tests import RESOURCE_DIR


def test_read_nt1():
    """
    Read from an RDF N-Triple file using RdfSource.
    """
    s = RdfSource()
    g = s.parse(os.path.join(RESOURCE_DIR, 'rdf', 'test1.nt'))
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1], rec[2])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    assert len(nodes) == 2
    assert len(edges) == 1

    n1 = nodes['ENSEMBL:ENSG0000000000001']
    assert n1['type'] == 'SO:0000704'
    assert len(n1['category']) == 4
    assert 'biolink:Gene' in n1['category']
    assert 'biolink:GenomicEntity' in n1['category']
    assert 'biolink:NamedThing' in n1['category']
    assert n1['name'] == 'Test Gene 123'
    assert n1['description'] == 'This is a Test Gene 123'
    assert 'Test Dataset' in n1['provided_by']

    n2 = nodes['ENSEMBL:ENSG0000000000002']
    assert n2['type'] == 'SO:0000704'
    assert len(n2['category']) == 4
    assert 'biolink:Gene' in n2['category']
    assert 'biolink:GenomicEntity' in n2['category']
    assert 'biolink:NamedThing' in n1['category']
    assert n2['name'] == 'Test Gene 456'
    assert n2['description'] == 'This is a Test Gene 456'
    assert 'Test Dataset' in n2['provided_by']

    e = list(edges.values())[0]
    assert e['subject'] == 'ENSEMBL:ENSG0000000000001'
    assert e['object'] == 'ENSEMBL:ENSG0000000000002'
    assert e['predicate'] == 'biolink:interacts_with'
    assert e['relation'] == 'biolink:interacts_with'


def test_read_nt2():
    """
    Read from an RDF N-Triple file using RdfSource.
    This test also supplies the provided_by parameter.
    """
    s = RdfSource()
    g = s.parse(os.path.join(RESOURCE_DIR, 'rdf', 'test1.nt'), provided_by='Test Dataset')
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1], rec[2])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    assert len(nodes) == 2
    assert len(edges) == 1

    n1 = nodes['ENSEMBL:ENSG0000000000001']
    assert n1['type'] == 'SO:0000704'
    assert len(n1['category']) == 4
    assert 'biolink:Gene' in n1['category']
    assert 'biolink:GenomicEntity' in n1['category']
    assert 'biolink:NamedThing' in n1['category']
    assert n1['name'] == 'Test Gene 123'
    assert n1['description'] == 'This is a Test Gene 123'
    assert 'Test Dataset' in n1['provided_by']

    n2 = nodes['ENSEMBL:ENSG0000000000002']
    assert n2['type'] == 'SO:0000704'
    assert len(n2['category']) == 4
    assert 'biolink:Gene' in n2['category']
    assert 'biolink:GenomicEntity' in n2['category']
    assert 'biolink:NamedThing' in n1['category']
    assert n2['name'] == 'Test Gene 456'
    assert n2['description'] == 'This is a Test Gene 456'
    assert 'Test Dataset' in n2['provided_by']

    e = list(edges.values())[0]
    assert e['subject'] == 'ENSEMBL:ENSG0000000000001'
    assert e['object'] == 'ENSEMBL:ENSG0000000000002'
    assert e['predicate'] == 'biolink:interacts_with'
    assert e['relation'] == 'biolink:interacts_with'
    assert 'Test Dataset' in e['provided_by']