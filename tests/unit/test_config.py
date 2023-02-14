import pytest

from kgx.config import get_biolink_model_schema


def test_valid_biolink_version():
    try:
        schema = get_biolink_model_schema("v3.2.1")
    except TypeError as te:
        assert False, "test failure!"
    assert (
        schema
        == "https://raw.githubusercontent.com/biolink/biolink-model/v3.2.1/biolink-model.yaml"
    )


def test_valid_biolink_version_no_v():
    try:
        schema = get_biolink_model_schema("2.0.1")
    except TypeError as te:
        assert False, "test failure!"
    assert (
        schema
        == "https://raw.githubusercontent.com/biolink/biolink-model/2.0.1/biolink-model.yaml"
    )


def test_invalid_biolink_version():
    try:
        schema = get_biolink_model_schema()
    except TypeError as te:
        assert (
            True
        ), "Type error expected: passed the invalid non-semver, type error: " + str(te)
