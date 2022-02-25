from sys import stderr

import pytest

from kgx.validator import Validator


@pytest.mark.parametrize("prefix", ["GO", "HP", "MONDO", "HGNC", "UniProtKB"])
def test_get_all_prefixes(prefix):
    """
    Test get_all_prefixes in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    prefixes = validator.get_all_prefixes()
    assert prefix in prefixes


@pytest.mark.parametrize("property", ["id", "category"])
def test_get_required_node_properties(property):
    """
    Test get_required_node_properties in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    properties = validator.get_required_node_properties()
    assert property in properties


@pytest.mark.parametrize("property", ["id", "subject", "object", "predicate"])
def test_get_required_edge_properties(property):
    """
    Test get_required_edge_properties in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    properties = validator.get_required_edge_properties()
    assert property in properties


@pytest.mark.parametrize(
    "query",
    [
        ("A:123", {}, False),
        ("A:123", {"id": "A:123"}, False),
        ("A:123", {"id": "A:123", "name": "Node A:123"}, False),
        (
            "A:123",
            {"id": "A:123", "name": "Node A:123", "category": ["biolink:NamedThing"]},
            True,
        ),
    ],
)
def test_validate_node_properties(query):
    """
    Test validate_node_properties in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    required_properties = validator.get_required_node_properties()
    validator.validate_node_properties(query[0], query[1], required_properties)
    assert (len(validator.get_errors()) == 0) == query[2]


@pytest.mark.parametrize(
    "query",
    [
        ("A:123", "X:1", {}, False),
        ("A:123", "X:1", {"predicate": "biolink:related_to"}, False),
        (
            "A:123",
            "X:1",
            {"subject": "A:123", "predicate": "biolink:related_to"},
            False,
        ),
        (
            "A:123",
            "X:1",
            {"subject": "A:123", "object": "X:1", "predicate": "biolink:related_to"},
            False,
        ),
        (
            "A:123",
            "X:1",
            {
                "id": "A:123-biolink:related_to-X:1",
                "subject": "A:123",
                "object": "X:1",
                "predicate": "biolink:related_to",
                "category": ["biolink:Association"],
            },
            True,
        ),
        (
            "A:123",
            "X:1",
            {
                "id": "Edge A-X",
                "subject": "A:123",
                "object": "X:1",
                "predicate": "biolink:related_to",
                "category": ["biolink:Association"],
            },
            True,
        ),
    ],
)
def test_validate_edge_properties(query):
    """
    Test validate_edge_properties in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    required_properties = validator.get_required_edge_properties()
    validator.validate_edge_properties(
        query[0], query[1], query[2], required_properties
    )
    
    # Dump a report to stderr ... will be a JSON document now
    print("\n*** validator error log:", file=stderr)
    validator.write_report()
    
    assert (len(validator.get_errors()) == 0) == query[3]


@pytest.mark.parametrize(
    "query",
    [
        (
            "A:123",
            {"id": "A:123", "name": "Node A:123", "category": ["biolink:NamedThing"]},
            True,
        ),
        (
            "A:123",
            {"id": "A:123", "name": "Node A:123", "category": "biolink:NamedThing"},
            False,
        ),
        (
            "A:123",
            {"id": "A:123", "name": ["Node A:123"], "category": "biolink:NamedThing"},
            False,
        ),
        (
            "A:123",
            {
                "id": "A:123",
                "name": "Node A:123",
                "category": ["biolink:NamedThing"],
                "publications": "PMID:789",
            },
            False,
        ),
        (
            "A:123",
            {
                "id": "A:123",
                "name": "Node A:123",
                "category": ["biolink:NamedThing"],
                "publications": ["PMID:789"],
            },
            True,
        ),
    ],
)
def test_validate_node_property_types(query):
    """
    Test validate_node_property_types in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    validator.validate_node_property_types(query[0], query[1])
    assert (len(validator.get_errors()) == 0) == query[2]


@pytest.mark.parametrize(
    "query",
    [
        (
                123,
                {"id": 123, "name": "Node A:123", "category": ["biolink:NamedThing"]},
                False,
        )
    ]
)
def test_validate_node_property_id_types_error(query):
    """
    Test validate_node_property_types in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    validator.validate_node_property_types(query[0], query[1])
    assert validator.get_errors() is not None


@pytest.mark.parametrize(
    "query",
    [
        (
                "a:123",
                {"id": "sweet:123", "name": "Node A:123", "category": 123},
                False,
        )
    ]
)
def test_validate_node_property_id_str_is_int_types_error(query):
    """
    Test validate_node_property_types in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    validator.validate_node_property_types(query[0], query[1])
    assert validator.get_errors() is not None


@pytest.mark.parametrize(
    "query",
    [
        (
                "a:123",
                {"id": 123, "name": "Node A:123", "category": ["biolink:NamedThing"]},
                False,
        )
    ]
)
def test_validate_node_property_uriorcurie_types_error(query):
    """
    Test validate_node_property_types in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    validator.validate_node_property_types(query[0], query[1])
    assert validator.get_errors() is not None


@pytest.mark.parametrize(
    "query",
    [
(
            "A:123",
            "X:1",
            {
                "id": "A:123",
                "subject": "a:123",
                "object": 123,
                "predicate": "biolink:related_to",
            },
            False,
        ),
    ],
)
def test_validate_edge_property_object_is_int_type_error(query):
    """
    Test validate_edge_property_types in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    validator.validate_edge_property_types(query[0], query[1], query[2])
    assert validator.get_default_model_version() is not None
    assert validator.get_errors() is not None


@pytest.mark.parametrize(
    "query",
    [
(
            "A:123",
            "X:1",
            {
                "id": "A:123",
                "subject": 123,
                "object": "X:1",
                "predicate": "biolink:related_to",
            },
            False,
        ),
    ],
)
def test_validate_edge_property_subject_is_int_type_error(query):
    """
    Test validate_edge_property_types in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    validator.validate_edge_property_types(query[0], query[1], query[2])
    assert validator.get_default_model_version() is not None
    assert validator.get_errors() is not None


@pytest.mark.parametrize(
    "query",
    [
        (
            "A:123",
            "X:1",
            {
                "id": "A:123-biolink:related_to-X:1",
                "subject": "A:123",
                "object": "X:1",
                "predicate": "biolink:related_to",
            },
            True,
        ),
        (
            "A:123",
            "X:1",
            {
                "id": "A:123-biolink:related_to-X:1",
                "subject": "A:123",
                "object": "X:1",
                "predicate": ["biolink:related_to"],
            },
            False,
        ),
        (
            "A:123",
            "X:1",
            {
                "id": "A:123-biolink:related_to-X:1",
                "subject": ["A:123"],
                "object": "X:1",
                "predicate": "biolink:related_to",
            },
            False,
        ),
        (
            "A:123",
            "X:1",
            {
                "subject": ["A:123"],
                "object": "X:1",
                "predicate": "biolink:related_to",
            },
            False,
        ),
    ],
)
def test_validate_edge_property_types_and_prefixes(query):
    """
    Test validate_edge_property_types in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    validator.validate_edge_property_types(query[0], query[1], query[2])
    assert validator.get_default_model_version() is not None
    assert (len(validator.get_errors()) == 0) == query[3]
    assert "biolink" in validator.get_all_prefixes()


@pytest.mark.parametrize(
    "query",
    [
        (
            "HGNC:123",
            {
                "id": "HGNC:123",
                "name": "Node HGNC:123",
                "category": ["biolink:NamedThing"],
            },
            True,
        ),
        (
            "HGNC_123",
            {
                "id": "HGNC_123",
                "name": "Node HGNC_123",
                "category": ["biolink:NamedThing"],
            },
            False,
        ),
        (
            "A:123",
            {"id": "A:123", "name": "Node A:123", "category": ["biolink:NamedThing"]},
            False,
        ),
    ],
)
def test_validate_node_property_values(query):
    """
    Test validate_node_property_values in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    validator.validate_node_property_values(query[0], query[1])
    assert (len(validator.get_errors()) == 0) == query[2]


@pytest.mark.parametrize(
    "query",
    [
        (
            "A:123",
            "X:1",
            {
                "id": "A:123-biolink:related_to-X:1",
                "subject": "A:123",
                "object": "X:1",
                "predicate": "biolink:related_to",
            },
            False,
        ),
        (
            "HGNC:123",
            "X:1",
            {
                "id": "HGNC:123-biolink:related_to-X:1",
                "subject": "A:123",
                "object": "X:1",
                "predicate": "biolink:related_to",
            },
            False,
        ),
        (
            "HGNC:123",
            "MONDO:1",
            {
                "id": "HGNC:123-biolink:related_to-MONDO:1",
                "subject": "A:123",
                "object": "X:1",
                "predicate": "biolink:related_to",
            },
            True,
        ),
        (
            "HGNC_123",
            "MONDO:1",
            {
                "id": "HGNC_123-biolink:related_to-MONDO:1",
                "subject": "A:123",
                "object": "X:1",
                "predicate": "biolink:related_to",
            },
            False,
        ),
    ],
)
def test_validate_edge_property_values(query):
    """
    Test validate_edge_property_values in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    validator.validate_edge_property_values(query[0], query[1], query[2])
    assert (len(validator.get_errors()) == 0) == query[3]


@pytest.mark.parametrize(
    "query",
    [
        (
            "HGNC:123",
            {
                "id": "HGNC:123",
                "name": "Node HGNC:123",
                "category": ["biolink:NamedThing"],
            },
            True,
        ),
        (
            "A:123",
            {
                "id": "A:123",
                "name": "Node A:123",
                "category": ["biolink:NamedThing", "biolink:Gene"],
            },
            True,
        ),
        (
            "A:123",
            {"id": "A:123", "name": "Node A:123", "category": ["NamedThing"]},
            True,
        ),
        ("A:123", {"id": "A:123", "name": "Node A:123", "category": ["Gene"]}, True),
        ("A:123", {"id": "A:123", "name": "Node A:123", "category": ["GENE"]}, False),
    ],
)
def test_validate_categories(query):
    """
    Test validate_categories in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    validator.validate_categories(query[0], query[1])
    assert (len(validator.get_errors()) == 0) == query[2]


@pytest.mark.parametrize(
    "query",
    [
        (
            "HGNC:123",
            "MONDO:1",
            {
                "id": "HGNC:123-biolink:related_to-MONDO:1",
                "subject": "A:123",
                "object": "X:1",
                "predicate": "biolink:related_to",
            },
            True,
        ),
        (
            "HGNC:123",
            "MONDO:1",
            {
                "id": "HGNC:123-biolink:related_to-MONDO:1",
                "subject": "A:123",
                "object": "X:1",
                "predicate": "related_to",
            },
            True,
        ),
        (
            "HGNC:123",
            "MONDO:1",
            {
                "id": "HGNC:123-biolink:related_to-MONDO:1",
                "subject": "A:123",
                "object": "X:1",
                "predicate": "related to",
            },
            False,
        ),
        (
            "HGNC:123",
            "MONDO:1",
            {
                "id": "HGNC:123-biolink:related_to-MONDO:1",
                "subject": "A:123",
                "object": "X:1",
                "predicate": "xyz",
            },
            False,
        ),
    ],
)
def test_validate_edge_label(query):
    """
    Test validate_edge_predicate in Validator.
    """
    validator = Validator.get_the_validator()
    validator.clear_errors()
    validator.validate_edge_predicate(query[0], query[1], dict(query[2]))
    assert (len(validator.get_errors()) == 0) == query[3]
