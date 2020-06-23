import os

from kgx import ObanRdfTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')


def test_load_ttl():
    t = ObanRdfTransformer()
    t.parse(os.path.join(resource_dir, 'hpoa_test.ttl'))
    assert t.graph.number_of_nodes() == 10
    assert t.graph.number_of_edges() == 5

    import pprint
    pprint.pprint([x for x in t.graph.nodes(data=True)])
    pprint.pprint([x for x in t.graph.edges(data=True)])

    n1 = t.graph.nodes(data=True)['HP:0000007']
    assert n1['id'] == 'HP:0000007'
    assert n1['provided_by'] == ['hpoa_test.ttl']
    assert n1['category'] == ['biolink:NamedThing']

    e1 = t.graph.get_edge_data('Orphanet:93262', 'HP:0000505')
    data = e1.popitem()
    assert data[1]['subject'] == 'Orphanet:93262'
    assert data[1]['edge_label'] == 'biolink:has_phenotype'
    assert data[1]['object'] == 'HP:0000505'
    assert data[1]['relation'] == 'RO:0002200'
    assert data[1]['provided_by'] == ['hpoa_test.ttl']
    assert data[1]['has_evidence'] == 'ECO:0000304'


def test_save_ttl():
    t = ObanRdfTransformer()
    t.parse(os.path.join(resource_dir, 'hpoa_test.ttl'))
    assert t.graph.number_of_nodes() == 10
    assert t.graph.number_of_edges() == 5

    t.save(os.path.join(target_dir, 'hpoa_test_export.ttl'))
    assert os.path.exists(os.path.join(target_dir, 'hpoa_test_export.ttl'))

    # TODO: test slot_uri

if __name__ == "__main__":
    test_load_ttl()