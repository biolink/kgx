import os

import pytest

from kgx import RdfTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')

@pytest.mark.skip()
def test_load_ttl():
    t = RdfTransformer()
    t.parse(os.path.join(resource_dir, 'hpoa_rdf_test.ttl'))
    import pprint
    pprint.pprint([x for x in t.graph.nodes(data=True)])
    pprint.pprint([x for x in t.graph.edges(data=True)])

@pytest.mark.skip()
def test_load_ttl2():
    t = RdfTransformer()
    t.parse(os.path.join(os.path.join(resource_dir, 'chembl_27.0_cellline.ttl')))
    import pprint
    pprint.pprint([x for x in t.graph.nodes(data=True)])
    pprint.pprint([x for x in t.graph.edges(data=True)])

@pytest.mark.skip()
def test_save_ttl():
    pass

@pytest.mark.parametrize("query", [
    (
        {'id': 'ABC:123', 'category': 'biolink:NamedThing', 'prop1': [1, 2, 3]},
        {'category': ['biolink:NamedThing', 'biolink:Gene'], 'prop1': [4]},
        ['biolink:NamedThing', 'biolink:Gene'],
        {'prop1': [1, 2, 3, 4]}
    ),
    (
        {'id': 'ABC:123', 'category': ['biolink:NamedThing'], 'prop1': 1},
        {'category': {'biolink:NamedThing', 'biolink:Gene'}, 'prop1': [2, 3]},
        ['biolink:NamedThing', 'biolink:Gene'],
        {'prop1': [1, 2, 3]}
    ),
    (
        {'id': 'ABC:123', 'category': ['biolink:NamedThing'], 'provided_by': 'test'},
        {'category': ('biolink:NamedThing', 'biolink:Gene'), 'provided_by': 'test'},
        ['biolink:NamedThing', 'biolink:Gene'],
        {'provided_by': ['test']}
    )
])
def test_prepare_data_dict(query):
    new_data = RdfTransformer._prepare_data_dict(query[0], query[1])
    assert new_data['category'] == query[2]
    for k, v in query[3].items():
        assert new_data[k] == v

