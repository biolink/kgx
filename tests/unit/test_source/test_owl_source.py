import os

from kgx.source import OwlSource
from tests import RESOURCE_DIR


def test_read_owl1():
    """
    Read an OWL ontology using OwlSource.
    """
    s = OwlSource()
    g = s.parse(os.path.join(RESOURCE_DIR, 'goslim_generic.owl'))
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    n1 = nodes['GO:0008150']
    assert n1['name'] == 'biological_process'
    assert 'synonym' in n1 and 'biological process' in n1['synonym']
    assert 'description' in n1
    assert 'comment' in n1
    assert 'has_alternative_id' in n1 and 'GO:0044699' in n1['has_alternative_id']

    n2 = nodes['GO:0003674']
    n2['name'] = 'molecular_function'
    assert 'synonym' in n2 and 'molecular function' in n2['synonym']
    assert 'description' in n2
    assert 'comment' in n2
    assert 'has_alternative_id' in n2 and 'GO:0005554' in n2['has_alternative_id']

    n3 = nodes['GO:0005575']
    n3['name'] = 'cellular_component'
    assert 'synonym' in n3 and 'cellular component' in n3['synonym']
    assert 'description' in n3
    assert 'comment' in n3
    assert 'has_alternative_id' in n3 and 'GO:0008372' in n3['has_alternative_id']

    e1 = edges['GO:0008289', 'GO:0003674']
    assert e1['subject'] == 'GO:0008289'
    assert e1['predicate'] == 'biolink:subclass_of'
    assert e1['object'] == 'GO:0003674'
    assert e1['relation'] == 'rdfs:subClassOf'


def test_read_owl2():
    """
    Read an OWL ontology using OwlSource.
    This test also supplies the provided_by parameter.
    """
    s = OwlSource()
    g = s.parse(os.path.join(RESOURCE_DIR, 'goslim_generic.owl'), provided_by='GO slim generic')
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    n1 = nodes['GO:0008150']
    assert n1['name'] == 'biological_process'
    assert 'synonym' in n1 and 'biological process' in n1['synonym']
    assert 'description' in n1
    assert 'comment' in n1
    assert 'has_alternative_id' in n1 and 'GO:0044699' in n1['has_alternative_id']
    assert 'GO slim generic' in n1['provided_by']

    n2 = nodes['GO:0003674']
    n2['name'] = 'molecular_function'
    assert 'synonym' in n2 and 'molecular function' in n2['synonym']
    assert 'description' in n2
    assert 'comment' in n2
    assert 'has_alternative_id' in n2 and 'GO:0005554' in n2['has_alternative_id']
    assert 'GO slim generic' in n2['provided_by']

    n3 = nodes['GO:0005575']
    n3['name'] = 'cellular_component'
    assert 'synonym' in n3 and 'cellular component' in n3['synonym']
    assert 'description' in n3
    assert 'comment' in n3
    assert 'has_alternative_id' in n3 and 'GO:0008372' in n3['has_alternative_id']
    assert 'GO slim generic' in n3['provided_by']

    e1 = edges['GO:0008289', 'GO:0003674']
    assert e1['subject'] == 'GO:0008289'
    assert e1['predicate'] == 'biolink:subclass_of'
    assert e1['object'] == 'GO:0003674'
    assert e1['relation'] == 'rdfs:subClassOf'
    assert 'GO slim generic' in e1['provided_by']