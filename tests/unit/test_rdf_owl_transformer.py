import os

import pytest

from kgx import RdfOwlTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')


@pytest.mark.skip()
def test_owl_load():
    t = RdfOwlTransformer()
    t.parse(os.path.join(resource_dir, 'goslim_generic.owl'))
    assert t.graph.number_of_nodes() == 10
    assert t.graph.number_of_edges() == 10


@pytest.mark.skip("KGX will not implement OWL export")
def test_owl_save():
    pass

