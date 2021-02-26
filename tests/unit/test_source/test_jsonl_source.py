import os

from kgx.source import JsonlSource
from tests import RESOURCE_DIR


def test_read_jsonl1():
    """
    Read from JSON Lines using JsonlSource.
    """
    s = JsonlSource()
    g = s.parse(os.path.join(RESOURCE_DIR, 'valid_nodes.jsonl'))
    nodes = {}
    for rec in g:
        if rec:
            nodes[rec[0]] = rec[1]

    g = s.parse(os.path.join(RESOURCE_DIR, 'valid_edges.jsonl'))
    edges = {}
    for rec in g:
        if rec:
            edges[(rec[0], rec[1])] = rec[3]

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


def test_read_jsonl2():
    """
    Read from JSON Lines using JsonlSource.
    This test also supplies the provided_by parameter.
    """
    s = JsonlSource()
    g = s.parse(os.path.join(RESOURCE_DIR, 'valid_nodes.jsonl'), provided_by='Test JSON')
    nodes = {}
    for rec in g:
        if rec:
            nodes[rec[0]] = rec[1]

    g = s.parse(os.path.join(RESOURCE_DIR, 'valid_edges.jsonl'), provided_by='Test JSON')
    edges = {}
    for rec in g:
        if rec:
            edges[(rec[0], rec[1])] = rec[3]

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
    assert 'Test JSON' in e['provided_by']
