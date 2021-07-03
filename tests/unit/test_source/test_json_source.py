import os

from kgx.source import JsonSource
from tests import RESOURCE_DIR


def test_read_json1():
    """
    Read from a JSON using JsonSource.
    """
    s = JsonSource()
    g = s.parse(os.path.join(RESOURCE_DIR, 'valid.json'))
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    assert len(nodes.keys()) == 6
    assert len(edges.keys()) == 5

    n = nodes['MONDO:0017148']
    assert 'id' in n and n['id'] == 'MONDO:0017148'
    assert n['name'] == 'heritable pulmonary arterial hypertension'
    assert n['category'][0] == 'biolink:Disease'

    e = edges[('HGNC:11603', 'MONDO:0017148')]
    assert e['subject'] == 'HGNC:11603'
    assert e['object'] == 'MONDO:0017148'
    assert e['predicate'] == 'biolink:related_to'
    assert e['relation'] == 'RO:0004013'


def test_read_json2():
    """
    Read from a JSON using JsonSource.
    This test also supplies the provided_by parameter.
    """
    s = JsonSource()
    g = s.parse(os.path.join(RESOURCE_DIR, 'valid.json'), provenance={'knowledge_source': 'Test JSON'})
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    assert len(nodes.keys()) == 6
    assert len(edges.keys()) == 5

    n = nodes['MONDO:0017148']
    assert 'id' in n and n['id'] == 'MONDO:0017148'
    assert n['name'] == 'heritable pulmonary arterial hypertension'
    assert n['category'][0] == 'biolink:Disease'
    assert 'Test JSON' in n['provided_by']

    e = edges[('HGNC:11603', 'MONDO:0017148')]
    assert e['subject'] == 'HGNC:11603'
    assert e['object'] == 'MONDO:0017148'
    assert e['predicate'] == 'biolink:related_to'
    assert e['relation'] == 'RO:0004013'
    assert 'Test JSON' in e['knowledge_source']


def test_read_json_compressed():
    """
    Read from a gzip compressed JSON using JsonSource.
    """
    s = JsonSource()
    g = s.parse(os.path.join(RESOURCE_DIR, 'valid.json.gz'), compression='gz')
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    assert len(nodes.keys()) == 6
    assert len(edges.keys()) == 5

    n = nodes['MONDO:0017148']
    assert 'id' in n and n['id'] == 'MONDO:0017148'
    assert n['name'] == 'heritable pulmonary arterial hypertension'
    assert n['category'][0] == 'biolink:Disease'

    e = edges[('HGNC:11603', 'MONDO:0017148')]
    assert e['subject'] == 'HGNC:11603'
    assert e['object'] == 'MONDO:0017148'
    assert e['predicate'] == 'biolink:related_to'
    assert e['relation'] == 'RO:0004013'
