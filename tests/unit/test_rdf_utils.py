import os
import pytest
from rdflib import URIRef, Graph
from pprint import pprint

from kgx.prefix_manager import PrefixManager
from kgx.utils.rdf_utils import infer_category, process_predicate
from tests import RESOURCE_DIR


@pytest.mark.parametrize(
    "query",
    [
        (URIRef("http://purl.obolibrary.org/obo/GO_0007267"), "biological_process"),
        (URIRef("http://purl.obolibrary.org/obo/GO_0019899"), "molecular_function"),
        (URIRef("http://purl.obolibrary.org/obo/GO_0005739"), "cellular_component"),
    ],
)
def test_infer_category(query):
    """
    Test inferring of biolink category for a given IRI.
    """
    graph = Graph()
    graph.parse(os.path.join(RESOURCE_DIR, "goslim_generic.owl"))
    [c] = infer_category(query[0], graph)
    assert c == query[1]


@pytest.mark.parametrize(
    "query",
    [
        (
            "http://purl.org/oban/association_has_object",
            "biolink:object",
            "rdf:object",
            "OBAN:association_has_object",
            "association_has_object",
        ),
        (
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "biolink:type",
            "rdf:type",
            "rdf:type",
            "type",
        ),
        (
            "https://monarchinitiative.org/frequencyOfPhenotype",
            None,
            None,
            "MONARCH:frequencyOfPhenotype",
            "frequencyOfPhenotype",
        ),
        (
            "http://purl.obolibrary.org/obo/RO_0002200",
            "biolink:has_phenotype",
            "biolink:has_phenotype",
            "RO:0002200",
            "0002200",
        ),
        (
            "http://www.w3.org/2002/07/owl#equivalentClass",
            "biolink:same_as",
            "biolink:same_as",
            "owl:equivalentClass",
            "equivalentClass",
        ),
        (
            "https://www.example.org/UNKNOWN/new_prop",
            None,
            None,
            ":new_prop",
            "new_prop",
        ),
        (
            "http://purl.obolibrary.org/obo/RO_0000091",
            None,
            None,
            "RO:0000091",
            "0000091",
        ),
        ("RO:0000091", None, None, "RO:0000091", "0000091"),
        ("category", "biolink:category", "biolink:category", ":category", "category"),
        ("predicate", "biolink:predicate", "rdf:predicate", ":predicate", "predicate"),
        ("type", "biolink:type", "rdf:type", ":type", "type"),
        ("name", "biolink:name", "rdfs:label", ":name", "name"),
    ],
)
def test_process_predicate(query):
    """
    Test behavior of process_predicate method.
    """
    pm = PrefixManager()
    pprint(query[0])
    x = process_predicate(pm, query[0])
    # x = "element_uri", "canonical_uri", "predicate", "property_name"
    # print("x: ", x)
    # print("query[0]", query[0])
    # print("x[0]: ", x[0], "query[1]: ", query[1])
    # print("x[1]: ", x[1], "query[2]: ", query[2])
    # print("x[2]: ", x[2], "query[3]: ", query[3])
    # print("x[3]: ", x[3], "query[4]: ", query[4])
    assert x[0] == query[1]
    assert x[1] == query[2]
    assert x[2] == query[3]
    assert x[3] == query[4]
