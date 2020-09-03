import os
import numpy as np
import pandas as pd
import pytest

from kgx import PandasTransformer

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')


def test_load_csv():
    t = PandasTransformer()
    t.parse(os.path.join(resource_dir, 'test_nodes.csv'), input_format='csv')
    t.parse(os.path.join(resource_dir, 'test_edges.csv'), input_format='csv')

    assert t.graph.number_of_nodes() == 3
    assert t.graph.number_of_edges() == 1

    assert t.graph.nodes['CURIE:123']['description'] == 'Node of type Gene, CURIE:123'
    assert t.graph.nodes['CURIE:456']['description'] == 'Node of type Disease, CURIE:456'

def test_load_tsv():
    t = PandasTransformer()
    t.parse(os.path.join(resource_dir, 'test_nodes.tsv'), input_format='tsv')
    t.parse(os.path.join(resource_dir, 'test_edges.tsv'), input_format='tsv')

    assert t.graph.number_of_nodes() == 3
    assert t.graph.number_of_edges() == 1

    assert t.graph.nodes['CURIE:123']['description'] == '"Node of type Gene, CURIE:123"'
    assert t.graph.nodes['CURIE:456']['description'] == '"Node of type Disease, CURIE:456"'


@pytest.mark.parametrize('query', [
    ('export', 'csv', None, ['export_nodes.csv', 'export_edges.csv']),
    ('export', 'tsv', None, ['export_nodes.tsv', 'export_edges.tsv']),
    ('export', 'tsv', 'tar', 'export.tar'),
    ('export', 'tsv', 'tar.gz', 'export.tar.gz')
])
def test_export(query):
    nodes = os.path.join(resource_dir, 'test_nodes.tsv')
    edges = os.path.join(resource_dir, 'test_edges.tsv')
    t = PandasTransformer()
    t.parse(nodes, input_format='tsv')
    t.parse(edges, input_format='tsv')

    assert t.graph.number_of_nodes() == 3
    assert t.graph.number_of_edges() == 1

    output_filename = os.path.join(target_dir, query[0])
    t.save(filename=output_filename, output_format=query[1], compression=query[2])

    if isinstance(query[3], str):
        assert os.path.exists(os.path.join(target_dir, query[3]))
    else:
        assert os.path.exists(os.path.join(target_dir, query[3][0]))
        assert os.path.exists(os.path.join(target_dir, query[3][1]))


@pytest.mark.parametrize('query', [
    (os.path.join(resource_dir, 'test.tar'), 'tsv', 'tar', 3, 1),
    (os.path.join(resource_dir, 'test.tar.gz'), 'tsv', 'tar.gz', 3, 1)
])
def test_load_compressed(query):
    t = PandasTransformer()
    t.parse(query[0], input_format=query[1], compression=query[2])
    assert t.graph.number_of_nodes() == query[3]
    assert t.graph.number_of_edges() == query[4]


@pytest.mark.parametrize('query', [
    ({'id': 'A', 'name': 'Node A'}, {'id': 'A', 'name': 'Node A'}),
    ({'id': 'A', 'name': 'Node A', 'description': None}, {'id': 'A', 'name': 'Node A'}),
    ({'id': 'A', 'name': 'Node A', 'description': None, 'publications': 'PMID:1234|PMID:1456|PMID:3466'}, {'id': 'A', 'name': 'Node A', 'publications': ['PMID:1234', 'PMID:1456', 'PMID:3466']}),
    ({'id': 'A', 'name': 'Node A', 'description': None, 'property': [pd.NA, 123, 'ABC']}, {'id': 'A', 'name': 'Node A', 'property': [123, 'ABC']})
])
def test_build_kwargs(query):
    d = PandasTransformer._build_kwargs(query[0])
    for k, v in query[1].items():
        assert k in d
        assert d[k] == v


@pytest.mark.parametrize('query', [
    ({'id': 'A', 'name': 'Node A', 'category': ['biolink:NamedThing', 'biolink:Gene']}, {'id': 'A', 'name': 'Node A', 'category': 'biolink:NamedThing|biolink:Gene'}),
    ({'id': 'A', 'name': 'Node A', 'category': ['biolink:NamedThing', 'biolink:Gene'], 'xrefs': [np.nan, 'UniProtKB:123', None, 'NCBIGene:456']}, {'id': 'A', 'name': 'Node A', 'category': 'biolink:NamedThing|biolink:Gene', 'xrefs': 'UniProtKB:123|NCBIGene:456'})
])
def test_build_export_row(query):
    d = PandasTransformer._build_export_row(query[0])
    for k, v in query[1].items():
        assert k in d
        assert d[k] == v


@pytest.mark.parametrize('query', [
    (('category', 'biolink:Gene'), ['biolink:Gene']),
    (('publications', 'PMID:123|PMID:456|PMID:789'), ['PMID:123', 'PMID:456', 'PMID:789']),
    (('negated', 'True'), True),
    (('negated', True), True),
    (('negated', True), True),
    (('xref', {'a', 'b', 'c'}), ['a', 'b', 'c']),
    (('xref', 'a|b|c'), ['a', 'b', 'c']),
    (('valid', 'True'), 'True'),
    (('valid', True), True),
    (('alias', 'xyz'), 'xyz'),
    (('description', 'Line 1\nLine 2\nLine 3'), 'Line 1 Line 2 Line 3'),
])
def test_sanitize_import(query):
    value = PandasTransformer._sanitize_import(query[0][0], query[0][1])
    if isinstance(query[1], str):
        assert value == query[1]
    elif isinstance(query[1], (list, set, tuple)):
        for x in query[1]:
            assert x in value
    elif isinstance(query[1], bool):
        assert query[1] == value
    else:
        assert query[1] in value


@pytest.mark.parametrize('query', [
    (('category', 'biolink:Gene'), ['biolink:Gene']),
    (('publications', ['PMID:123', 'PMID:456', 'PMID:789']), 'PMID:123|PMID:456|PMID:789'),
    (('negated', 'True'), True),
    (('negated', True), True),
    (('negated', True), True),
    (('xref', {'a', 'b', 'c'}), ['a', 'b', 'c']),
    (('xref', ['a', 'b', 'c']), 'a|b|c'),
    (('valid', 'True'), 'True'),
    (('valid', True), True),
    (('alias', 'xyz'), 'xyz'),
    (('description', 'Line 1\nLine 2\nLine 3'), 'Line 1 Line 2 Line 3'),
])
def test_sanitize_export(query):
    value = PandasTransformer._sanitize_export(query[0][0], query[0][1])
    if isinstance(query[1], str):
        assert value == query[1]
    elif isinstance(query[1], (list, set, tuple)):
        for x in query[1]:
            assert x in value
    elif isinstance(query[1], bool):
        assert query[1] == value
    else:
        assert query[1] in value


@pytest.mark.parametrize('query', [
    (
        {'category': {'biolink:Gene', 'biolink:Disease'}},
        {},
        2,
        1
    ),
    (
        {'category': {'biolink:Gene', 'biolink:Disease', 'biolink:PhenotypicFeature'}},
        {'validated': 'true'},
        3,
        2
    ),
    (
        {'category': {'biolink:Gene', 'biolink:PhenotypicFeature'}},
        {'subject_category': {'biolink:Gene'}, 'object_category': {'biolink:PhenotypicFeature'}, 'edge_label': {'biolink:related_to'}},
        2,
        1
    ),
])
def test_filters(query):
    nodes = os.path.join(resource_dir, 'test2_nodes.tsv')
    edges = os.path.join(resource_dir, 'test2_edges.tsv')
    t = PandasTransformer()
    for nf in query[0].keys():
        t.set_node_filter(nf, query[0][nf])

    for ef in query[1].keys():
        t.set_edge_filter(ef, query[1][ef])

    t.parse(nodes, input_format='tsv')
    t.parse(edges, input_format='tsv')
    assert t.graph.number_of_nodes() == query[2]
    assert t.graph.number_of_edges() == query[3]


@pytest.mark.parametrize('query', [
    (
        {},
        {},
        512,
        532
    ),
    (
        {'category': {'biolink:Gene'}},
        {},
        178,
        178
    ),
    (
        {'category': {'biolink:Gene'}},
        {'subject_category': {'biolink:Gene'}, 'object_category': {'biolink:Gene'}},
        178,
        178
    ),
    (
        {'category': {'biolink:Gene'}},
        {'subject_category': {'biolink:Gene'}, 'object_category': {'biolink:Gene'}, 'edge_label': {'biolink:orthologous_to'}},
        178,
        13
    ),
    (
        {'category': {'biolink:Gene'}},
        {'edge_label': {'biolink:interacts_with'}},
        178,
        165
    ),
    (
        {},
        {'provided_by': {'omim', 'hpoa', 'orphanet'}},
        512,
        166
    ),
    (
        {},
        {'subject_category': {'biolink:Disease'}},
        56,
        35
    ),
    (
        {},
        {'object_category': {'biolink:Disease'}},
        22,
        20
    )
])
def test_filters_graph(query):
    nodes = os.path.join(resource_dir, 'graph_nodes.tsv')
    edges = os.path.join(resource_dir, 'graph_edges.tsv')
    t = PandasTransformer()
    for nf in query[0].keys():
        t.set_node_filter(nf, query[0][nf])

    for ef in query[1].keys():
        t.set_edge_filter(ef, query[1][ef])

    t.parse(nodes, input_format='tsv', **{'lineterminator': None})
    t.parse(edges, input_format='tsv', **{'lineterminator': None})
    assert t.graph.number_of_nodes() == query[2]
    assert t.graph.number_of_edges() == query[3]
