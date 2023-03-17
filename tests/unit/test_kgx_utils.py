import pytest
import pandas as pd
import numpy as np
from bmt import Toolkit

from kgx.curie_lookup_service import CurieLookupService
from kgx.utils.kgx_utils import (
    get_toolkit,
    get_curie_lookup_service,
    get_prefix_prioritization_map,
    get_biolink_element,
    get_biolink_ancestors,
    generate_edge_key,
    contract,
    expand,
    camelcase_to_sentencecase,
    snakecase_to_sentencecase,
    sentencecase_to_snakecase,
    sentencecase_to_camelcase,
    generate_uuid,
    prepare_data_dict,
    sanitize_import,
    build_export_row,
    _sanitize_import_property,
    _sanitize_export_property,
)


def test_get_toolkit():
    """
    Test to get an instance of Toolkit via get_toolkit and
    check if default is the default biolink model version.
    """
    tk = get_toolkit()
    assert isinstance(tk, Toolkit)
    assert tk.get_model_version() == Toolkit().get_model_version()


def test_get_curie_lookup_service():
    """
    Test to get an instance of CurieLookupService via get_curie_lookup_service.
    """
    cls = get_curie_lookup_service()
    assert isinstance(cls, CurieLookupService)


def test_get_prefix_prioritization_map():
    """
    Test to get a prefix prioritization map.
    """
    prioritization_map = get_prefix_prioritization_map()
    assert "biolink:Gene" in prioritization_map.keys()
    assert "biolink:Protein" in prioritization_map.keys()
    assert "biolink:Disease" in prioritization_map.keys()


@pytest.mark.parametrize(
    "query",
    [
        ("gene", "gene"),
        ("disease", "disease"),
        ("related_to", "related to"),
        ("causes", "causes"),
        ("biolink:Gene", "gene"),
        ("biolink:causes", "causes"),
    ],
)
def test_get_biolink_element(query):
    """
    Test to get biolink element.
    """
    element1 = get_biolink_element(query[0])
    assert element1 is not None
    assert element1.name == query[1]


def test_get_biolink_ancestors():
    """
    Test to get biolink ancestors.
    """
    ancestors1 = get_biolink_ancestors("phenotypic feature")
    assert ancestors1 is not None
    # changed to 6 from 5 when biolink model updated to 2.2.1 and mixins are included in ancestry
    assert len(ancestors1) == 6


def test_generate_edge_key():
    """
    Test generation of edge key via generate_edge_key method.
    """
    key = generate_edge_key("S:CURIE", "related_to", "O:CURIE")
    assert key == "S:CURIE-related_to-O:CURIE"


def test_camelcase_to_sentencecase():
    """
    Test conversion of CamelCase to sentence case.
    """
    s = camelcase_to_sentencecase("NamedThing")
    assert s == "named thing"


def test_snakecase_to_sentencecase():
    """
    Test conversion of a snake_case to sentence case.
    """
    s = snakecase_to_sentencecase("named_thing")
    assert s == "named thing"


def test_sentencecase_to_snakecase():
    """
    Test conversion of a sentence case text to snake_case.
    """
    s = sentencecase_to_snakecase("named thing")
    assert s == "named_thing"


def test_sentencecase_to_camelcase():
    """
    Test conversion of a sentence case text to CamelCase.
    """
    s = sentencecase_to_camelcase("named thing")
    assert s == "NamedThing"


@pytest.mark.parametrize(
    "query",
    [
        (
            "HGNC:11603",
            "http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=11603",
            "https://identifiers.org/hgnc:11603",
        )
    ],
)
def test_contract(query):
    """
    Test contract method for contracting an IRI to a CURIE.
    """
    curie = contract(query[1], prefix_maps=None, fallback=True)
    # get the CURIE
    assert curie == query[0]

    # provide custom prefix_maps, with fallback
    curie = contract(
        query[2], prefix_maps=[{"HGNC": "https://identifiers.org/hgnc:"}], fallback=True
    )
    # get the CURIE
    assert curie == query[0]

    # provide custom prefix_maps, but no fallback
    curie = contract(
        query[2],
        prefix_maps=[{"HGNC": "https://identifiers.org/hgnc:"}],
        fallback=False,
    )
    # get the CURIE
    assert curie == query[0]

    # provide no prefix_maps, and no fallback
    curie = contract(query[2], prefix_maps=None, fallback=False)
    # get back the IRI
    assert curie == query[2]


@pytest.mark.parametrize(
    "query",
    [
        (
            "HGNC:11603",
            "http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=11603",
            "https://identifiers.org/hgnc:11603",
        )
    ],
)
def test_expand(query):
    """
    Test expand method for expanding a CURIE to an IRI.
    """
    iri = expand(query[0], prefix_maps=None, fallback=True)
    # get the IRI
    assert iri == query[1]

    # provide custom prefix_maps, with fallback
    iri = expand(
        query[0], prefix_maps=[{"HGNC": "https://identifiers.org/hgnc:"}], fallback=True
    )
    # get the alternate IRI
    assert iri == query[2]

    # provide custom prefix_maps, but no fallback
    iri = expand(
        query[0], prefix_maps=[{"hgnc": "https://example.org/hgnc:"}], fallback=False
    )
    # get back the CURIE
    assert iri == query[0]

    # provide no prefix_maps, and no fallback
    iri = expand(query[0], prefix_maps=None, fallback=False)
    # get the IRI
    assert iri == query[1]


def test_generate_uuid():
    """
    Test generation of UUID by generate_uuid method.
    """
    s = generate_uuid()
    assert s.startswith("urn:uuid:")


@pytest.mark.parametrize(
    "query",
    [
        (
            {"id": "HGNC:11603", "name": "Some Gene", "provided_by": ["Dataset A"]},
            {"id": "HGNC:11603", "name": "Some Gene", "provided_by": "Dataset B"},
            {"id": "HGNC:11603", "name": "Some Gene", "provided_by": ["Dataset C"]},
        ),
        (
            {"id": "HGNC:11603", "name": "Some Gene", "provided_by": ["Dataset A"]},
            {"id": "HGNC:11603", "name": "Some Gene", "provided_by": "Dataset B"},
            {},
        ),
    ],
)
def test_prepare_data_dict(query):
    """
    Test prepare_data_dict method.
    """
    res = prepare_data_dict(query[0], query[1])
    res = prepare_data_dict(res, query[2])
    assert res is not None


@pytest.mark.parametrize(
    "query",
    [
        ({"id": "A", "name": "Node A"}, {"id": "A", "name": "Node A"}),
        (
            {"id": "A", "name": "Node A", "description": None},
            {"id": "A", "name": "Node A"},
        ),
        (
            {
                "id": "A",
                "name": "Node A",
                "description": None,
                "publications": "PMID:1234|PMID:1456|PMID:3466",
            },
            {
                "id": "A",
                "name": "Node A",
                "publications": ["PMID:1234", "PMID:1456", "PMID:3466"],
            },
        ),
        (
            {
                "id": "A",
                "name": "Node A",
                "description": None,
                "property": [pd.NA, 123, "ABC"],
            },
            {"id": "A", "name": "Node A", "property": [123, "ABC"]},
        ),
        (
            {
                "id": "A",
                "name": "Node A",
                "description": None,
                "property": [pd.NA, 123, "ABC"],
                "score": 0.0,
            },
            {"id": "A", "name": "Node A", "property": [123, "ABC"], "score": 0.0},
        ),
    ],
)
def test_sanitize_import1(query):
    """
    Test sanitize_import method.
    """
    d = sanitize_import(query[0], list_delimiter='|')
    for k, v in query[1].items():
        assert k in d
        if isinstance(v, list):
            assert set(d[k]) == set(v)
        else:
            assert d[k] == v


@pytest.mark.parametrize(
    "query",
    [
        (("category", "biolink:Gene"), ["biolink:Gene"]),
        (
            ("publications", "PMID:123|PMID:456|PMID:789"),
            ["PMID:123", "PMID:456", "PMID:789"],
        ),
        (("negated", "True"), True),
        (("negated", True), True),
        (("negated", True), True),
        (("xref", {"a", "b", "c"}), ["a", "b", "c"]),
        (("xref", "a|b|c"), ["a", "b", "c"]),
        (("valid", "True"), "True"),
        (("valid", True), True),
        (("alias", "xyz"), "xyz"),
        (("description", "Line 1\nLine 2\nLine 3"), "Line 1 Line 2 Line 3"),
    ],
)
def test_sanitize_import2(query):
    """
    Test internal sanitize_import method.
    """
    value = _sanitize_import_property(query[0][0], query[0][1], list_delimiter='|')
    if isinstance(query[1], str):
        assert value == query[1]
    elif isinstance(query[1], (list, set, tuple)):
        for x in query[1]:
            assert x in value
    elif isinstance(query[1], bool):
        assert query[1] == value
    else:
        assert query[1] in value


@pytest.mark.parametrize(
    "query",
    [
        (
            {
                "id": "A",
                "name": "Node A",
                "category": ["biolink:NamedThing", "biolink:Gene"],
            },
            {
                "id": "A",
                "name": "Node A",
                "category": "biolink:NamedThing|biolink:Gene",
            },
        ),
        (
            {
                "id": "A",
                "name": "Node A",
                "category": ["biolink:NamedThing", "biolink:Gene"],
                "xrefs": [np.nan, "UniProtKB:123", None, "NCBIGene:456"],
            },
            {
                "id": "A",
                "name": "Node A",
                "category": "biolink:NamedThing|biolink:Gene",
                "xrefs": "UniProtKB:123|NCBIGene:456",
            },
        ),
    ],
)
def test_build_export_row(query):
    """
    Test build_export_row method.
    """
    d = build_export_row(query[0], list_delimiter="|")
    for k, v in query[1].items():
        assert k in d
        assert d[k] == v


@pytest.mark.parametrize(
    "query",
    [
        (("category", "biolink:Gene"), ["biolink:Gene"]),
        (
            ("publications", ["PMID:123", "PMID:456", "PMID:789"]),
            "PMID:123|PMID:456|PMID:789",
        ),
        (("negated", "True"), True),
        (("negated", True), True),
        (("negated", True), True),
        (("xref", {"a", "b", "c"}), ["a", "b", "c"]),
        (("xref", ["a", "b", "c"]), "a|b|c"),
        (("valid", "True"), "True"),
        (("valid", True), True),
        (("alias", "xyz"), "xyz"),
        (("description", "Line 1\nLine 2\nLine 3"), "Line 1 Line 2 Line 3"),
    ],
)
def test_sanitize_export_property(query):
    """
    Test sanitize_export method.
    """
    value = _sanitize_export_property(query[0][0], query[0][1], list_delimiter='|')
    if isinstance(query[1], str):
        assert value == query[1]
    elif isinstance(query[1], (list, set, tuple)):
        for x in query[1]:
            assert x in value
    elif isinstance(query[1], bool):
        assert query[1] == value
    else:
        assert query[1] in value
