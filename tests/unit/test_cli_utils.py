import json
import os
import pytest

from kgx.cli.cli_utils import validate, neo4j_upload, neo4j_download, transform, merge
from kgx.cli import (
    get_input_file_types,
    graph_summary,
    get_report_format_types
)
from tests import RESOURCE_DIR, TARGET_DIR
from tests.unit import (
    clean_slate,
    check_container,
    CONTAINER_NAME,
    DEFAULT_NEO4J_URL,
    DEFAULT_NEO4J_USERNAME,
    DEFAULT_NEO4J_PASSWORD,
)


def test_get_file_types():
    """
    Test get_file_types method.
    """
    file_types = get_input_file_types()
    assert 'tsv' in file_types
    assert 'nt' in file_types
    assert 'json' in file_types
    assert 'obojson' in file_types


def test_get_report_format_types():
    """
    Test get_report_format_types method.
    """
    format_types = get_report_format_types()
    assert 'yaml' in format_types
    assert 'json' in format_types


def test_graph_summary1():
    """
    Test graph summary, where the output report type is kgx-map.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
        os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
    ]
    output = os.path.join(TARGET_DIR, 'graph_stats1.yaml')
    summary_stats = graph_summary(inputs, 'tsv', None, output, report_type='kgx-map')

    assert os.path.exists(output)
    assert summary_stats
    assert 'node_stats' in summary_stats
    assert 'edge_stats' in summary_stats
    assert summary_stats['node_stats']['total_nodes'] == 512
    assert 'biolink:Gene' in summary_stats['node_stats']['node_categories']
    assert 'biolink:Disease' in summary_stats['node_stats']['node_categories']
    assert summary_stats['edge_stats']['total_edges'] == 540
    assert 'biolink:has_phenotype' in summary_stats['edge_stats']['predicates']
    assert 'biolink:interacts_with' in summary_stats['edge_stats']['predicates']


def test_graph_summary2a():
    """
     Test graph summary, where the output report type
     is meta-knowledge-graph, default JSON report format type.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
        os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
    ]
    output = os.path.join(TARGET_DIR, 'graph_stats2.json')
    summary_stats = graph_summary(
        inputs,
        'tsv',
        None,
        output,
        report_type='meta-knowledge-graph'
    )

    assert os.path.exists(output)
    assert summary_stats
    assert 'nodes' in summary_stats
    assert 'edges' in summary_stats


def test_graph_summary2b():
    """
     Test graph summary, where the output report type
     is meta-knowledge-graph, set as YAML report format type.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
        os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
    ]
    output = os.path.join(TARGET_DIR, 'graph_stats2.yaml')
    summary_stats = graph_summary(
        inputs,
        'tsv',
        None,
        output,
        report_type='meta-knowledge-graph',
        report_format='yaml'
    )

    assert os.path.exists(output)
    assert summary_stats
    assert 'nodes' in summary_stats
    assert 'edges' in summary_stats
    
def test_graph_summary2c():
    """
     Test graph summary, where the output report type
     is meta-knowledge-graph, set as YAML report format type.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
        os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
    ]
    output = os.path.join(TARGET_DIR, 'graph_stats2c.json')
    summary_stats = graph_summary(
        inputs=inputs,
        input_format='tsv',
        input_compression=None,
        output=output,
        report_type='meta-knowledge-graph',
        stream=True
    )

    assert os.path.exists(output)
    assert summary_stats
    assert 'nodes' in summary_stats
    assert 'edges' in summary_stats


def test_validate_non_streaming():
    """
    Test graph validation.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, 'valid.json'),
    ]
    output = os.path.join(TARGET_DIR, 'validation.log')
    errors = validate(
        inputs=inputs,
        input_format='json',
        input_compression=None,
        output=output,
        stream=False
    )
    assert os.path.exists(output)
    assert len(errors) == 0


def test_validate_streaming():
    """
    Test graph validation.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, 'valid.json'),
    ]
    output = os.path.join(TARGET_DIR, 'validation.log')
    errors = validate(
        inputs=inputs,
        input_format='json',
        input_compression=None,
        output=output,
        stream=True
    )
    assert os.path.exists(output)
    assert len(errors) == 0


@pytest.mark.skipif(not check_container(), reason=f'Container {CONTAINER_NAME} is not running')
def test_neo4j_upload(clean_slate):
    """
    Test upload to Neo4j.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
        os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
    ]
    # upload
    t = neo4j_upload(
        inputs,
        'tsv',
        None,
        uri=DEFAULT_NEO4J_URL,
        username=DEFAULT_NEO4J_USERNAME,
        password=DEFAULT_NEO4J_PASSWORD,
        stream=False,
    )
    assert t.store.graph.number_of_nodes() == 512
    assert t.store.graph.number_of_edges() == 532


@pytest.mark.skip()
@pytest.mark.skipif(not check_container(), reason=f'Container {CONTAINER_NAME} is not running')
def test_neo4j_download(clean_slate):
    """
    Test download from Neo4j.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
        os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
    ]
    output = os.path.join(TARGET_DIR, 'neo_download')
    # upload
    t1 = neo4j_upload(
        inputs=inputs,
        input_format='tsv',
        input_compression=None,
        uri=DEFAULT_NEO4J_URL,
        username=DEFAULT_NEO4J_USERNAME,
        password=DEFAULT_NEO4J_PASSWORD,
        stream=False,
    )
    t2 = neo4j_download(
        uri=DEFAULT_NEO4J_URL,
        username=DEFAULT_NEO4J_USERNAME,
        password=DEFAULT_NEO4J_PASSWORD,
        output=output,
        output_format='tsv',
        output_compression=None,
        stream=False,
    )
    assert os.path.exists(f"{output}_nodes.tsv")
    assert os.path.exists(f"{output}_edges.tsv")
    assert t1.store.graph.number_of_nodes() == t2.store.graph.number_of_nodes()
    assert t1.store.graph.number_of_edges() == t2.store.graph.number_of_edges()


def test_transform1():
    """
    Transform graph from TSV to JSON.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'),
        os.path.join(RESOURCE_DIR, 'graph_edges.tsv'),
    ]
    output = os.path.join(TARGET_DIR, 'graph.json')
    transform(
        inputs=inputs,
        input_format='tsv',
        input_compression=None,
        output=output,
        output_format='json',
        output_compression=None,
    )
    assert os.path.exists(output)
    data = json.load(open(output, 'r'))
    assert 'nodes' in data
    assert 'edges' in data
    assert len(data['nodes']) == 512
    assert len(data['edges']) == 532


def test_transform2():
    """
    Transform from a test transform YAML.
    """
    transform_config = os.path.join(RESOURCE_DIR, 'test-transform.yaml')
    transform(inputs=None, transform_config=transform_config)
    assert os.path.exists(os.path.join(RESOURCE_DIR, 'graph_nodes.tsv'))
    assert os.path.exists(os.path.join(RESOURCE_DIR, 'graph_edges.tsv'))


def test_merge1():
    """
    Transform from test merge YAML.
    """
    merge_config = os.path.join(RESOURCE_DIR, 'test-merge.yaml')
    merge(merge_config=merge_config)
    assert os.path.join(TARGET_DIR, 'merged-graph_nodes.tsv')
    assert os.path.join(TARGET_DIR, 'merged-graph_edges.tsv')
    assert os.path.join(TARGET_DIR, 'merged-graph.json')


def test_merge2():
    """
    Transform selected source from test merge YAML and
    write selected destinations.
    """
    merge_config = os.path.join(RESOURCE_DIR, 'test-merge.yaml')
    merge(merge_config=merge_config, destination=['merged-graph-json'])
    assert os.path.join(TARGET_DIR, 'merged-graph.json')
