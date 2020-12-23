import os
import pprint

import pytest

from kgx import RdfOwlTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')


def test_owl_parse1():
    t = RdfOwlTransformer()
    t.parse(os.path.join(resource_dir, 'goslim_generic.owl'))
    n1 = t.graph.nodes()['GO:0008150']
    assert n1['biolink:name'] == 'biological_process'
    assert 'biolink:synonym' in n1 and 'biological process' in n1['biolink:synonym']
    assert 'biolink:description' in n1
    assert 'comment' in n1
    assert 'has_alternative_id' in n1 and 'GO:0044699' in n1['has_alternative_id']

    n2 = t.graph.nodes()['GO:0003674']
    n2['biolink:name'] = 'molecular_function'
    assert 'biolink:synonym' in n2 and 'molecular function' in n2['biolink:synonym']
    assert 'biolink:description' in n2
    assert 'comment' in n2
    assert 'has_alternative_id' in n2 and 'GO:0005554' in n2['has_alternative_id']

    n3 = t.graph.nodes()['GO:0005575']
    n3['biolink:name'] = 'cellular_component'
    assert 'biolink:synonym' in n3 and 'cellular component' in n3['biolink:synonym']
    assert 'biolink:description' in n3
    assert 'comment' in n3
    assert 'has_alternative_id' in n3 and 'GO:0008372' in n3['has_alternative_id']

    e1 = list(t.graph.get_edge('GO:0008289', 'GO:0003674').values())[0]
    assert e1['biolink:subject'] == 'GO:0008289'
    assert e1['biolink:predicate'] == 'biolink:subclass_of'
    assert e1['biolink:object'] == 'GO:0003674'
    assert e1['biolink:relation'] == 'rdfs:subClassOf'


def test_owl_parse2():
    np = {
        'http://www.geneontology.org/formats/oboInOwl#inSubset',
    }
    predicate_mapping = {
        'http://www.geneontology.org/formats/oboInOwl#inSubset': 'subsets',
        'http://www.geneontology.org/formats/oboInOwl#hasOBONamespace': 'namespace',
        'http://www.geneontology.org/formats/oboInOwl#hasAlternativeId': 'biolink:xref',
    }
    t = RdfOwlTransformer()
    t.set_predicate_mapping(predicate_mapping)
    t.parse(os.path.join(resource_dir, 'goslim_generic.owl'), node_property_predicates=np)

    n1 = t.graph.nodes()['GO:0008150']
    print(n1)
    assert n1['biolink:name'] == 'biological_process'
    assert 'subsets' in n1 and 'GOP:goslim_generic' in n1['subsets']
    assert 'biolink:synonym' in n1 and 'biological process' in n1['biolink:synonym']
    assert 'biolink:description' in n1
    assert 'comment' in n1
    assert 'biolink:xref' in n1 and 'GO:0044699' in n1['biolink:xref']

    n2 = t.graph.nodes()['GO:0003674']
    assert n2['biolink:name'] == 'molecular_function'
    assert 'subsets' in n2 and 'GOP:goslim_generic' in n2['subsets']
    assert 'biolink:synonym' in n2 and 'molecular function' in n2['biolink:synonym']
    assert 'biolink:description' in n2
    assert 'comment' in n2
    assert 'biolink:xref' in n2 and 'GO:0005554' in n2['biolink:xref']

    n3 = t.graph.nodes()['GO:0005575']
    assert n3['biolink:name'] == 'cellular_component'
    assert 'subsets' in n3 and 'GOP:goslim_generic' in n3['subsets']
    assert 'biolink:synonym' in n3 and 'cellular component' in n3['biolink:synonym']
    assert 'biolink:description' in n3
    assert 'comment' in n3
    assert 'biolink:xref' in n3 and 'GO:0008372' in n3['biolink:xref']

    e1 = list(t.graph.get_edge('GO:0008289', 'GO:0003674').values())[0]
    assert e1['biolink:subject'] == 'GO:0008289'
    assert e1['biolink:predicate'] == 'biolink:subclass_of'
    assert e1['biolink:object'] == 'GO:0003674'
    assert e1['biolink:relation'] == 'rdfs:subClassOf'


@pytest.mark.skip("KGX will not implement OWL export")
def test_owl_save():
    pass

