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
