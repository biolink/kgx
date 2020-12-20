import json
import os
import pprint
from time import sleep

import pytest
from kgx.cli.cli_utils import validate, neo4j_upload, neo4j_download, transform, merge

from kgx import PandasTransformer, JsonTransformer, RdfTransformer, NtTransformer
from kgx.cli import get_transformer, get_file_types, graph_summary
from tests import clean_slate, check_container, CONTAINER_NAME, DEFAULT_NEO4J_URL, DEFAULT_NEO4J_USERNAME, DEFAULT_NEO4J_PASSWORD

cwd = os.path.abspath(os.path.dirname(__file__))
resource_dir = os.path.join(cwd, '../resources')
target_dir = os.path.join(cwd, '../target')


def test_get_transformer():
    t = get_transformer('tsv')
    assert t == PandasTransformer

    t = get_transformer('json')
    assert t == JsonTransformer

    t = get_transformer('nt')
    assert t == NtTransformer

    t = get_transformer('ttl')
    assert t == RdfTransformer


def test_get_file_types():
    file_types = get_file_types()
    assert 'tsv' in file_types
    assert 'nt' in file_types
    assert 'json' in file_types
    assert 'ttl' in file_types


def test_graph_summary():
    inputs = [
        os.path.join(resource_dir, 'graph_nodes.tsv'),
        os.path.join(resource_dir, 'graph_edges.tsv')
    ]
    output = os.path.join(target_dir, 'graph_stats.yaml')
    summary_stats = graph_summary(inputs, 'tsv', None, output)
    pprint.pprint(summary_stats)

    assert os.path.exists(output)
    assert summary_stats
    assert 'node_stats' in summary_stats
    assert 'edge_stats' in summary_stats
    assert summary_stats['node_stats']['total_nodes'] == 512
    assert 'biolink:Gene' in summary_stats['node_stats']['node_categories']
    assert 'biolink:Disease' in summary_stats['node_stats']['node_categories']
    assert summary_stats['edge_stats']['total_edges'] == 532
    assert 'biolink:has_phenotype' in summary_stats['edge_stats']['predicates']
    assert 'biolink:interacts_with' in summary_stats['edge_stats']['predicates']


def test_validate():
    inputs = [
        os.path.join(resource_dir, 'valid.json'),
    ]
    output = os.path.join(target_dir, 'validation.log')
    errors = validate(inputs, 'json', None, output)
    assert os.path.exists(output)
    assert len(errors) == 0


@pytest.mark.skipif(not check_container(), reason=f'Container {CONTAINER_NAME} is not running')
def test_neo4j_upload(clean_slate):
    inputs = [
        os.path.join(resource_dir, 'graph_nodes.tsv'),
        os.path.join(resource_dir, 'graph_edges.tsv')
    ]
    # upload
    t = neo4j_upload(inputs, 'tsv', None, uri=DEFAULT_NEO4J_URL, username=DEFAULT_NEO4J_USERNAME, password=DEFAULT_NEO4J_PASSWORD)
    t.report()


@pytest.mark.skipif(not check_container(), reason=f'Container {CONTAINER_NAME} is not running')
def test_neo4j_download(clean_slate):
    inputs = [
        os.path.join(resource_dir, 'graph_nodes.tsv'),
        os.path.join(resource_dir, 'graph_edges.tsv')
    ]
    output = os.path.join(target_dir, 'neo_download')
    # upload
    t1 = neo4j_upload(
        inputs=inputs,
        input_format='tsv',
        input_compression=None,
        uri=DEFAULT_NEO4J_URL,
        username=DEFAULT_NEO4J_USERNAME,
        password=DEFAULT_NEO4J_PASSWORD
    )
    t1.report()
    t2 = neo4j_download(
        uri=DEFAULT_NEO4J_URL,
        username=DEFAULT_NEO4J_USERNAME,
        password=DEFAULT_NEO4J_PASSWORD,
        output=output,
        output_format='tsv',
        output_compression=None
    )
    t2.report()
    assert os.path.exists(f"{output}_nodes.tsv")
    assert os.path.exists(f"{output}_edges.tsv")
    assert t1.graph.number_of_nodes() == t2.graph.number_of_nodes()
    assert t1.graph.number_of_edges() == t2.graph.number_of_edges()


def test_transform1():
    # transform graph from TSV to JSON
    inputs = [
        os.path.join(resource_dir, 'graph_nodes.tsv'),
        os.path.join(resource_dir, 'graph_edges.tsv')
    ]
    output = os.path.join(target_dir, 'graph.json')
    transform(
        inputs=inputs,
        input_format='tsv',
        input_compression=None,
        output=output,
        output_format='json',
        output_compression=None
    )
    assert os.path.exists(output)
    data = json.load(open(output, 'r'))
    assert 'nodes' in data
    assert 'edges' in data
    assert len(data['nodes']) == 512
    assert len(data['edges']) == 532


def test_transform2():
    # transform from a test transform yaml
    transform_config = os.path.join(resource_dir, 'test-transform.yaml')
    transform(None, transform_config=transform_config)
    assert os.path.exists(os.path.join(resource_dir, 'graph_nodes.tsv'))
    assert os.path.exists(os.path.join(resource_dir, 'graph_edges.tsv'))


def test_merge1():
    # transform from test merge yaml
    merge_config = os.path.join(resource_dir, 'test-merge.yaml')
    merge(merge_config=merge_config)
    assert os.path.join(target_dir, 'merged-graph_nodes.tsv')
    assert os.path.join(target_dir, 'merged-graph_edges.tsv')
    assert os.path.join(target_dir, 'merged-graph.json')


def test_merge2():
    # transform selected source from test merge yaml and
    # write selected destinations
    merge_config = os.path.join(resource_dir, 'test-merge.yaml')
    merge(merge_config=merge_config, destination=['merged-graph-json'])
    assert os.path.join(target_dir, 'merged-graph.json')

