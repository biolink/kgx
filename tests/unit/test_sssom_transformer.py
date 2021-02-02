import os

from kgx.transformers.sssom_transformer import SssomTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')


def test_load1():
    t = SssomTransformer()
    t.parse(os.path.join(resource_dir, 'sssom_example1.tsv'))

    assert t.graph.number_of_nodes() == 18
    assert t.graph.number_of_edges() == 9

    assert t.graph.nodes()['MP:0012051']['id'] == 'MP:0012051'
    assert t.graph.nodes()['HP:0001257']['id'] == 'HP:0001257'

    e = list(dict(t.graph.get_edge('MP:0012051', 'HP:0001257')).values())[0]
    assert e['subject'] == 'MP:0012051'
    assert e['object'] == 'HP:0001257'
    assert e['predicate'] == 'biolink:same_as'
    assert e['confidence'] == '1.0'


def test_load2():
    t = SssomTransformer()
    t.parse(os.path.join(resource_dir, 'sssom_example2.tsv'))

    assert t.graph.number_of_nodes() == 18
    assert t.graph.number_of_edges() == 9

    n1 = t.graph.nodes()['MP:0002152']
    assert n1['id'] == 'MP:0002152'
    assert n1['name'] == 'abnormal brain morphology'

    n2 = t.graph.nodes()['HP:0012443']
    assert n2['id'] == 'HP:0012443'
    assert n2['name'] == 'Abnormality of brain morphology'

    e = list(dict(t.graph.get_edge('MP:0002152', 'HP:0012443')).values())[0]
    assert e['subject'] == 'MP:0002152'
    assert e['object'] == 'HP:0012443'
    assert e['predicate'] == 'biolink:exact_match'
    assert e['match_type'] == 'SSSOMC:Lexical'
    assert e['reviewer_id'] == 'orcid:0000-0000-0000-0000'



