import os

from kgx.source import TrapiSource
from tests import RESOURCE_DIR


def test_read_trapi_json1():
    """
    Read from a JSON using TrapiSource.
    """
    s = TrapiSource()
    g = s.parse(os.path.join(RESOURCE_DIR, 'rsa_sample.json'))
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    assert len(nodes.keys()) == 4
    assert len(edges.keys()) == 3

    n = nodes['HGNC:11603']
    assert n['id'] == 'HGNC:11603'
    assert n['name'] == 'TBX4'
    assert n['category'] == ['biolink:Gene']

    e = edges['HGNC:11603', 'MONDO:0005002']
    assert e['subject'] == 'HGNC:11603'
    assert e['object'] == 'MONDO:0005002'
    assert e['predicate'] == 'biolink:related_to'


def test_read_trapi_json2():
    """
    Read from a TRAPI JSON using TrapiSource.
    This test also supplies the knowledge_source parameter.
    """
    s = TrapiSource()
    g = s.parse(
        os.path.join(RESOURCE_DIR, 'rsa_sample.json'),
        provenance={
            'provided_by': "Test TRAPI JSON",
            'knowledge_source': "Test TRAPI JSON"
        }
    )
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    assert len(nodes.keys()) == 4
    assert len(edges.keys()) == 3

    n = nodes['HGNC:11603']
    assert n['id'] == 'HGNC:11603'
    assert n['name'] == 'TBX4'
    assert n['category'] == ['biolink:Gene']
    assert 'Test TRAPI JSON' in n['provided_by']

    e = edges['HGNC:11603', 'MONDO:0005002']
    assert e['subject'] == 'HGNC:11603'
    assert e['object'] == 'MONDO:0005002'
    assert e['predicate'] == 'biolink:related_to'
    assert 'Test TRAPI JSON' in e['knowledge_source']
