import os
import pytest
from rdflib import URIRef, Graph

from kgx.utils.rdf_utils import infer_category, generate_uuid

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')


@pytest.mark.parametrize('query', [
    (URIRef('http://purl.obolibrary.org/obo/GO_0007267'), 'biological_process'),
    (URIRef('http://purl.obolibrary.org/obo/GO_0019899'), 'molecular_function'),
    (URIRef('http://purl.obolibrary.org/obo/GO_0005739'), 'cellular_component')
])
def test_infer_category(query):
    graph = Graph()
    graph.parse(os.path.join(resource_dir, 'goslim_generic.owl'))
    [c] = infer_category(query[0], graph)
    assert c == query[1]


def test_generate_uuid():
    s = generate_uuid()
    assert s.startswith('urn:uuid:')
