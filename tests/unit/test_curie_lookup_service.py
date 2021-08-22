import pytest

from kgx.curie_lookup_service import CurieLookupService


@pytest.mark.parametrize(
    "query",
    [
        ("RO:0002410", "causally_related_to"),
        ("RO:0002334", "regulated_by"),
        ("BFO:0000003", "occurrent"),
    ],
)
def test_curie_lookup(query):
    """
    Test lookup for a given CURIE via CurieLookupService.
    """
    cls = CurieLookupService()
    assert len(cls.ontologies) > 0
    assert query[0] in cls.ontology_graph
    assert query[0] in cls.curie_map
    assert cls.curie_map[query[0]] == query[1]


def test_curie_lookup_with_custom():
    """
    Test lookup for a given CURIE via CurieLookupService, with a user defined
    CURIE prefix map.
    """
    cls = CurieLookupService(curie_map={"XYZ:123": "custom entry"})
    assert len(cls.ontologies) > 0
    assert "XYZ:123" in cls.curie_map
    assert cls.curie_map["XYZ:123"] == "custom entry"
