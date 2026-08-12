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
        # OBO ontologies with no entry of their own in the JSON-LD context. The
        # context's catch-all `OBO -> http://purl.obolibrary.org/obo/` matches
        # them, and must not suppress the specific mappings in obo_context.
        ("http://purl.obolibrary.org/obo/DDPHENO_0000001", "DDPHENO:0000001"),
        ("http://purl.obolibrary.org/obo/FBbt_00000001", "FBbt:00000001"),
        ("http://purl.obolibrary.org/obo/EMAPA_16040", "EMAPA:16040"),
        ("http://purl.obolibrary.org/obo/WBbt_0005733", "WBbt:0005733"),
        ("http://purl.obolibrary.org/obo/ZFA_0000001", "ZFA:0000001"),
        ("http://purl.obolibrary.org/obo/XAO_0000001", "XAO:0000001"),
        ("http://purl.obolibrary.org/obo/ZFS_0000001", "ZFS:0000001"),
        ("http://purl.obolibrary.org/obo/CHR_0000001", "CHR:0000001"),
        # Ontologies that do have their own context entry are unaffected.
        ("http://purl.obolibrary.org/obo/HP_0000001", "HP:0000001"),
        ("http://purl.obolibrary.org/obo/MONDO_0000001", "MONDO:0000001"),
        ("http://purl.obolibrary.org/obo/RO_0002162", "RO:0002162"),
        ("http://purl.obolibrary.org/obo/BFO_0000050", "BFO:0000050"),
        # IRIs beneath the OBO namespace that no per-ontology mapping covers
        # keep the catch-all, as before.
        (
            "http://purl.obolibrary.org/obo/fbbt#has_function_in",
            "OBO:fbbt#has_function_in",
        ),
    ],
)
def test_prefix_manager_contract_obo_idspaces(query):
    """
    Test that the OBO catch-all does not shadow per-ontology mappings.
    """
    pm = PrefixManager()
    assert pm.contract(query[0]) == query[1]
