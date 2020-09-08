import pytest

from kgx import PandasTransformer, JsonTransformer, RdfTransformer, NtTransformer
from kgx.cli import get_transformer, get_file_types


def test_get_transformer():
    t = get_transformer('tsv')
    assert t == PandasTransformer

    t = get_transformer('json')
    assert t == JsonTransformer

    t = get_transformer('nt')
    assert t == NtTransformer

    t = get_transformer('rdf')
    assert t == RdfTransformer


def test_get_file_types():
    file_types = get_file_types()
    assert 'tsv' in file_types
    assert 'nt' in file_types
    assert 'json' in file_types
    assert 'rdf' in file_types
