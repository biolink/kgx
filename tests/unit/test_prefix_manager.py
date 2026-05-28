import pytest

from kgx.prefix_manager import PrefixManager


@pytest.mark.parametrize(
    "query",
    [
        ("https://example.org/123", True),
        ("http://example.org/ABC", True),
        ("http://purl.obolibrary.org/obo/GO_0008150", True),
        ("GO:0008150", False),
    ],
)
def test_is_iri(query):
    """
    Test to check behavior of is_iri method in PrefixManager.
    """
    assert PrefixManager.is_iri(query[0]) == query[1]


@pytest.mark.parametrize(
    "query",
    [
        ("GO:0008150", True),
        ("CHEMBL.COMPOUND:12345", True),
        ("HP:0000000", True),
        ("GO_0008150", False),
        ("12345", False),
        (":12345", True),
    ],
)
def test_is_curie(query):
    """
    Test to check behavior of is_curie method in PrefixManager.
    """
    assert PrefixManager.is_curie(query[0]) == query[1]


@pytest.mark.parametrize(
    "query",
    [
        ("GO:0008150", "GO"),
        ("CHEMBL.COMPOUND:12345", "CHEMBL.COMPOUND"),
        ("HP:0000000", "HP"),
        ("GO_0008150", None),
        ("12345", None),
        (":12345", ""),
    ],
)
def test_get_prefix(query):
    """
    Test to check behavior of test_get_prefix method in PrefixManager.
    """
    assert PrefixManager.get_prefix(query[0]) == query[1]


@pytest.mark.parametrize(
    "query",
    [
        ("GO:0008150", "0008150"),
        ("CHEMBL.COMPOUND:12345", "12345"),
        ("HP:0000000", "0000000"),
        ("GO_0008150", None),
        ("12345", None),
        (":12345", "12345"),
    ],
)
def test_get_reference(query):
    """
    Test to check behavior of get_reference method in PrefixManager.
    """
    assert PrefixManager.get_reference(query[0]) == query[1]


def test_prefix_manager():
    """
    Test to get an instance of PrefixManager.
    """
    pm = PrefixManager()
    assert pm.prefix_map
    assert pm.reverse_prefix_map
    assert "biolink" in pm.prefix_map
    assert "" in pm.prefix_map


@pytest.mark.parametrize(
    "query",
    [
        ("GO:0008150", "http://purl.obolibrary.org/obo/GO_0008150"),
        ("HP:0000000", "http://purl.obolibrary.org/obo/HP_0000000"),
        ("biolink:category", "https://w3id.org/biolink/vocab/category"),
        ("biolink:related_to", "https://w3id.org/biolink/vocab/related_to"),
        ("biolink:NamedThing", "https://w3id.org/biolink/vocab/NamedThing"),
        ("HGNC:1103", "http://identifiers.org/hgnc/1103"),
    ],
)
def test_prefix_manager_expand(query):
    """
    Test to check the expand method in PrefixManager.
    """
    pm = PrefixManager()
    assert pm.expand(query[0]) == query[1]


@pytest.mark.parametrize(
    "query",
    [
        ("http://purl.obolibrary.org/obo/GO_0008150", "GO:0008150"),
        ("http://purl.obolibrary.org/obo/HP_0000000", "HP:0000000"),
        ("https://w3id.org/biolink/vocab/category", "biolink:category"),
        ("https://w3id.org/biolink/vocab/related_to", "biolink:related_to"),
        ("https://w3id.org/biolink/vocab/NamedThing", "biolink:NamedThing"),
        ("http://identifiers.org/hgnc/1103", "HGNC:1103"),
    ],
)
def test_prefix_manager_contract(query):
    """
    Test to check the contract method in PrefixManager.
    """
    pm = PrefixManager()
    assert pm.contract(query[0]) == query[1]


@pytest.mark.parametrize(
    "query",
    [
        # OBO-hosted ontologies that aren't in the biolink default JSON-LD context
        # but ARE in the prefixcommons monarch/obo contexts. The wildcard
        # OBO: <http://purl.obolibrary.org/obo/> in the biolink context must NOT
        # shadow the more-specific match available via the fallback contexts.
        ("http://purl.obolibrary.org/obo/FBbt_00000001", "FBbt:00000001"),
        ("http://purl.obolibrary.org/obo/WBbt_0000100", "WBbt:0000100"),
        ("http://purl.obolibrary.org/obo/ZFA_0000000", "ZFA:0000000"),
        ("http://purl.obolibrary.org/obo/XAO_0000000", "XAO:0000000"),
        ("http://purl.obolibrary.org/obo/OBA_0000001", "OBA:0000001"),
        ("http://purl.obolibrary.org/obo/EMAPA_0000001", "EMAPA:0000001"),
        ("http://purl.obolibrary.org/obo/DDPHENO_0000001", "DDPHENO:0000001"),
    ],
)
def test_prefix_manager_contract_longest_match(query):
    """
    Regression: when multiple registered prefixes can contract a URI, prefer
    the most-specific (longest IRI) mapping rather than letting the wildcard
    OBO: prefix swallow ontology-specific IDs.
    """
    pm = PrefixManager()
    assert pm.contract(query[0]) == query[1]
