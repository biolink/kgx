"""
Test CLI Utils
"""
import csv
import json
import os
import pytest
from click.testing import CliRunner
from pprint import pprint
from kgx.cli.cli_utils import validate, neo4j_upload, neo4j_download, merge, get_output_file_types
from kgx.cli import cli, get_input_file_types, graph_summary, get_report_format_types, transform
from tests import RESOURCE_DIR, TARGET_DIR
from tests.unit import (
    check_container,
    CONTAINER_NAME,
    DEFAULT_NEO4J_URL,
    DEFAULT_NEO4J_USERNAME,
    DEFAULT_NEO4J_PASSWORD,
    clean_database
)


def test_get_file_types():
    """
    Test get_file_types method.
    """
    file_types = get_input_file_types()
    assert "tsv" in file_types
    assert "nt" in file_types
    assert "json" in file_types
    assert "obojson" in file_types


def test_get_report_format_types():
    """
    Test get_report_format_types method.
    """
    format_types = get_report_format_types()
    assert "yaml" in format_types
    assert "json" in format_types


def test_graph_summary_wrapper():
    output = os.path.join(TARGET_DIR, "graph_stats3.yaml")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "graph-summary",
            "-i", "tsv",
            "-o", output,
            os.path.join(RESOURCE_DIR, "graph_nodes.tsv")
        ]
    )
    assert result.exit_code == 0


def test_graph_summary_wrapper_error():
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "graph_stats3.yaml")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "graph-summary",
            "-i", "tsv",
            "-o", output,
            inputs
        ]
    )
    assert result.exit_code == 1


def test_graph_summary_report_type_wrapper_error():
    output = os.path.join(TARGET_DIR, "graph_stats3.yaml")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "graph-summary",
            "-i", "tsv",
            "-o", output,
            "-r", "testoutput",
            os.path.join(RESOURCE_DIR, "graph_nodes.tsv")
        ]
    )
    assert result.exit_code == 1


def test_graph_summary_report_format_wrapper_error():
    output = os.path.join(TARGET_DIR, "graph_stats3.yaml")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "graph-summary",
            "-i", "tsv",
            "-o", output,
            "-f", "notaformat",
            os.path.join(RESOURCE_DIR, "graph_nodes.tsv")
        ]
    )
    assert result.exit_code == 1


def test_transform_wrapper():
    """
        Transform graph from TSV to JSON.
        """
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "grapht.json")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "transform",
            "-i", "tsv",
            "-o", output,
            "-f", "json",
            inputs
        ]
    )

    assert result.exit_code == 1


def test_transform_uncompressed_tsv_to_tsv():
    """
        Transform nodes and edges file to nodes and edges TSV file
        with extra provenance
        """

    inputs = [
        os.path.join(RESOURCE_DIR, "chebi_kgx_tsv_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "chebi_kgx_tsv_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "chebi_snippet")

    knowledge_sources = [
        ("aggregator_knowledge_source", "someks"),
        ("primary_knowledge_source", "someotherks"),
        ("knowledge_source", "newknowledge")
    ]
    transform(
        inputs=inputs,
        input_format="tsv",
        input_compression=None,
        output=output,
        output_format="tsv",
        output_compression=None,
        knowledge_sources=knowledge_sources,
    )

    assert os.path.exists(f"{output}_nodes.tsv")
    assert os.path.exists(f"{output}_edges.tsv")

    with open(f"{output}_edges.tsv", "r") as fd:
        edges = csv.reader(fd, delimiter="\t", quotechar='"')
        csv_headings = next(edges)
        assert "aggregator_knowledge_source" in csv_headings
        for row in edges:
            print(row)
            assert len(row) == 12
            assert "someks" in row
            assert "someotherks" in row
            assert "newknowledge" not in row
            assert "chebiasc66dwf" in row


def test_transform_obojson_to_csv_wrapper():
    """
        Transform obojson to CSV.
        """

    inputs = [
        os.path.join(RESOURCE_DIR, "BFO_2_relaxed.json")
    ]
    output = os.path.join(TARGET_DIR, "test_bfo_2_relaxed")
    knowledge_sources = [
        ("aggregator_knowledge_source", "bioportal"),
        ("primary_knowledge_source", "justastring")
    ]
    transform(
        inputs=inputs,
        input_format="obojson",
        input_compression=None,
        output=output,
        output_format="tsv",
        output_compression=None,
        knowledge_sources=knowledge_sources,
    )

    with open(f"{output}_edges.tsv", "r") as fd:
        edges = csv.reader(fd, delimiter="\t", quotechar='"')
        csv_headings = next(edges)
        assert "aggregator_knowledge_source" in csv_headings
        for row in edges:
            assert "bioportal" in row
            assert "justastring" in row


def test_transform_with_provided_by_obojson_to_csv_wrapper():
    """
        Transform obojson to CSV.
        """

    inputs = [
        os.path.join(RESOURCE_DIR, "BFO_2_relaxed.json")
    ]
    output = os.path.join(TARGET_DIR, "test_bfo_2_relaxed_provided_by.csv")
    knowledge_sources = [
        ("aggregator_knowledge_source", "bioportal"),
        ("primary_knowledge_source", "justastring"),
        ("provided_by", "bioportal")
    ]
    transform(
        inputs=inputs,
        input_format="obojson",
        input_compression=None,
        output=output,
        output_format="tsv",
        output_compression=None,
        knowledge_sources=knowledge_sources,
    )


def test_merge_wrapper():

    """
    Transform from test merge YAML.
    """
    merge_config = os.path.join(RESOURCE_DIR, "test-merge.yaml")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "merge",
            "--merge-config", merge_config
        ]
    )

    assert result.exit_code == 0
    assert os.path.join(TARGET_DIR, "merged-graph_nodes.tsv")
    assert os.path.join(TARGET_DIR, "merged-graph_edges.tsv")
    assert os.path.join(TARGET_DIR, "merged-graph.json")


def test_get_output_file_types():
    format_types = get_output_file_types()
    assert format_types is not None


def test_merge_wrapper_error():

    """
    Transform from test merge YAML.
    """
    merge_config = os.path.join(RESOURCE_DIR, "test-merge.yaml")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "merge"
        ]
    )

    assert result.exit_code == 2


def test_kgx_graph_summary():
    """
    Test graph summary, where the output report type is kgx-map.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "graph_stats1.yaml")
    summary_stats = graph_summary(
        inputs,
        "tsv",
        None,
        output,
        node_facet_properties=["provided_by"],
        edge_facet_properties=["aggregator_knowledge_source"],
        report_type="kgx-map"
    )

    assert os.path.exists(output)
    assert summary_stats
    assert "node_stats" in summary_stats
    assert "edge_stats" in summary_stats
    assert summary_stats["node_stats"]["total_nodes"] == 512
    assert "biolink:Gene" in summary_stats["node_stats"]["node_categories"]
    assert "biolink:Disease" in summary_stats["node_stats"]["node_categories"]
    assert summary_stats["edge_stats"]["total_edges"] == 539
    assert "biolink:has_phenotype" in summary_stats["edge_stats"]["predicates"]
    assert "biolink:interacts_with" in summary_stats["edge_stats"]["predicates"]


def test_chebi_tsv_to_tsv_transform():

    inputs = [
        os.path.join(RESOURCE_DIR, "chebi_kgx_tsv.tar.gz")
    ]
    output = os.path.join(TARGET_DIR, "test_chebi.tsv")

    knowledge_sources = [
        ("aggregator_knowledge_source", "test1"),
        ("primary_knowledge_source", "test2")
    ]

    transform(inputs=inputs,
              input_format='tsv',
              input_compression='tar.gz',
              output=output,
              output_format='tsv',
              knowledge_sources=knowledge_sources)


def test_meta_knowledge_graph_as_json():
    """
    Test graph summary, where the output report type is a meta-knowledge-graph,
    with results output as the default JSON report format type.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "meta-knowledge-graph.json")
    summary_stats = graph_summary(
        inputs,
        "tsv",
        None,
        output,
        report_type="meta-knowledge-graph",
        node_facet_properties=["provided_by"],
        edge_facet_properties=["aggregator_knowledge_source"],
        graph_name="Default Meta-Knowledge-Graph",
    )

    assert os.path.exists(output)
    assert summary_stats
    assert "nodes" in summary_stats
    assert "edges" in summary_stats
    assert "name" in summary_stats
    assert summary_stats["name"] == "Default Meta-Knowledge-Graph"


def test_meta_knowledge_graph_as_yaml():
    """
    Test graph summary, where the output report type is a meta-knowledge-graph,
    with results output as the YAML report output format type.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "meta-knowledge-graph.yaml")
    summary_stats = graph_summary(
        inputs,
        "tsv",
        None,
        output,
        report_type="meta-knowledge-graph",
        node_facet_properties=["provided_by"],
        edge_facet_properties=["aggregator_knowledge_source"],
        report_format="yaml"
    )

    assert os.path.exists(output)
    assert summary_stats
    assert "nodes" in summary_stats
    assert "edges" in summary_stats


def test_meta_knowledge_graph_as_json_streamed():
    """
    Test graph summary processed in stream mode, where the output report type
    is meta-knowledge-graph, output as the default JSON report format type.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "meta-knowledge-graph-streamed.json")
    summary_stats = graph_summary(
        inputs=inputs,
        input_format="tsv",
        input_compression=None,
        output=output,
        report_type="meta-knowledge-graph",
        node_facet_properties=["provided_by"],
        edge_facet_properties=["aggregator_knowledge_source"]
    )

    assert os.path.exists(output)
    assert summary_stats
    assert "nodes" in summary_stats
    assert "edges" in summary_stats


def test_validate_exception_triggered_error_exit_code():
    """
    Test graph validate error exit code.
    """
    test_input = os.path.join(RESOURCE_DIR, "graph_tiny_nodes.tsv")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "-i", "tsv",
            "-b not.a.semver",
            test_input
        ]
    )
    assert result.exit_code == 2


@pytest.mark.parametrize(
    "query",
    [
        ("graph_nodes.tsv", 0),
        ("test_nodes.tsv", 1),
    ],
)
def test_validate_parsing_triggered_error_exit_code(query):
    """
    Test graph validate error exit code.
    """
    test_input = os.path.join(RESOURCE_DIR, query[0])
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "validate",
            "-i", "tsv",
            test_input
        ]
    )
    assert result.exit_code == query[1]


def test_validate():
    """
    Test graph validation.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, "valid.json"),
    ]
    output = os.path.join(TARGET_DIR, "validation.log")
    errors = validate(
        inputs=inputs,
        input_format="json",
        input_compression=None,
        output=output
    )
    assert os.path.exists(output)
    assert len(errors) == 0


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_neo4j_upload(clean_database):
    """
    Test upload to Neo4j.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
    ]
    # upload
    t = neo4j_upload(
        inputs,
        "tsv",
        None,
        uri=DEFAULT_NEO4J_URL,
        username=DEFAULT_NEO4J_USERNAME,
        password=DEFAULT_NEO4J_PASSWORD,
        stream=False,
    )
    assert t.store.graph.number_of_nodes() == 512
    assert t.store.graph.number_of_edges() == 531


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_neo4j_download_wrapper(clean_database):
    output = os.path.join(TARGET_DIR, "neo_download2")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "neo4j-download",
            "-l", DEFAULT_NEO4J_URL,
            "-o", output,
            "-f", "tsv",
            "-u", DEFAULT_NEO4J_USERNAME,
            "-p", DEFAULT_NEO4J_PASSWORD,
        ]
    )

    assert os.path.exists(f"{output}_nodes.tsv")
    assert os.path.exists(f"{output}_edges.tsv")

    assert result.exit_code == 0


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_download_exception_triggered_error_exit_code():
    """
    Test graph download error exit code.
    """

    output = os.path.join(TARGET_DIR, "neo_download")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "neo4j-download",
            "-l", DEFAULT_NEO4J_URL,
            "-o", output,
            "-f", "tsvr",
            "-u", "not a user name",
            "-p", DEFAULT_NEO4J_PASSWORD,
        ]
    )
    assert result.exit_code == 1


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_neo4j_upload_wrapper(clean_database):
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
    ]
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "neo4j-upload",
            "--input-format", "tsv",
            "--uri", DEFAULT_NEO4J_URL,
            "--username", DEFAULT_NEO4J_USERNAME,
            "--password", DEFAULT_NEO4J_PASSWORD,
            os.path.join(RESOURCE_DIR, "graph_nodes.tsv")
        ]
    )

    assert result.exit_code == 0


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_neo4j_upload_wrapper_error(clean_database):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "neo4j-upload",
            "-i", "tsv",
            "inputs", "not_a_path"
            "-u", "not a user",
            "-p", DEFAULT_NEO4J_PASSWORD,
        ]
    )

    assert result.exit_code == 2


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_neo4j_download(clean_database):
    """
    Test download from Neo4j.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "neo_download")
    # upload
    t1 = neo4j_upload(
        inputs=inputs,
        input_format="tsv",
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
        output_format="tsv",
        output_compression=None,
        stream=False,
    )
    assert os.path.exists(f"{output}_nodes.tsv")
    assert os.path.exists(f"{output}_edges.tsv")
    assert t1.store.graph.number_of_nodes() == t2.store.graph.number_of_nodes()
    assert t1.store.graph.number_of_edges() == t2.store.graph.number_of_edges()


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_neo4j_download(clean_database):
    """
    Test download from Neo4j.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "neo_download")
    # upload
    t1 = neo4j_upload(
        inputs=inputs,
        input_format="tsv",
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
        output_format="",
        output_compression=None,
        stream=False,
    )
    assert os.path.exists(f"{output}_nodes.tsv")


def test_transform1():
    """
    Transform graph from TSV to JSON.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "graph.json")
    knowledge_sources = [
        ("aggregator_knowledge_source", "True"),
    ]
    transform(
        inputs=inputs,
        input_format="tsv",
        input_compression=None,
        output=output,
        output_format="json",
        output_compression=None,
        knowledge_sources=knowledge_sources,
    )
    assert os.path.exists(output)
    data = json.load(open(output, "r"))
    assert "nodes" in data
    assert "edges" in data
    assert len(data["nodes"]) == 512
    assert len(data["edges"]) == 531
    for e in data["edges"]:
        if e["subject"] == "HGNC:10848" and e["object"] == "HGNC:20738":
            assert "aggregator_knowledge_source" in e
            assert "infores:string" in e["aggregator_knowledge_source"]
            assert "infores:biogrid" in e["aggregator_knowledge_source"]
            break


def test_transform_error():
    """
    Transform graph from TSV to JSON.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "graph.json")
    knowledge_sources = [
        ("aggregator_knowledge_source", "True"),
    ]
    try: {
        transform(
            transform_config="out.txt",
            inputs=inputs,
            input_format="tsv",
            input_compression=None,
            output=output,
            output_format="json",
            output_compression=None,
            knowledge_sources=knowledge_sources,
        )
    }
    except ValueError:
        assert ValueError


def test_transform_knowledge_source_suppression():
    """
    Transform graph from TSV to JSON.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "graph.json")
    knowledge_sources = [
        ("aggregator_knowledge_source", "False"),
        ("knowledge_source", "False"),
    ]
    transform(
        inputs=inputs,
        input_format="tsv",
        input_compression=None,
        output=output,
        output_format="json",
        output_compression=None,
        knowledge_sources=knowledge_sources,
    )
    assert os.path.exists(output)
    data = json.load(open(output, "r"))
    assert "nodes" in data
    assert "edges" in data
    assert len(data["nodes"]) == 512
    assert len(data["edges"]) == 531
    for e in data["edges"]:
        if e["subject"] == "HGNC:10848" and e["object"] == "HGNC:20738":
            assert "aggregator_knowledge_source" not in e
            assert "knowledge_source" not in e
            break


def test_transform_provided_by_suppression():
    """
    Transform graph from TSV to JSON.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "graph.json")
    knowledge_sources = [
        ("aggregator_knowledge_source", "False"),
        ("knowledge_source", "False"),
        ("provided_by", "False")
    ]
    transform(
        inputs=inputs,
        input_format="tsv",
        input_compression=None,
        output=output,
        output_format="json",
        output_compression=None,
        knowledge_sources=knowledge_sources,
    )
    assert os.path.exists(output)
    data = json.load(open(output, "r"))
    for n in data["nodes"]:
        assert "provided_by" not in n


def test_transform_knowledge_source_rewrite():
    """
    Transform graph from TSV to JSON.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_tiny_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_tiny_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "graph.json")
    knowledge_sources = [
        ("aggregator_knowledge_source", "go,gene ontology"),
        ("aggregator_knowledge_source", "string,string database"),
    ]
    transform(
        inputs=inputs,
        input_format="tsv",
        input_compression=None,
        output=output,
        output_format="json",
        output_compression=None,
        knowledge_sources=knowledge_sources,
    )
    assert os.path.exists(output)
    data = json.load(open(output, "r"))
    assert "nodes" in data
    assert "edges" in data
    assert len(data["nodes"]) == 6
    assert len(data["edges"]) == 9
    for e in data["edges"]:
        if e["subject"] == "HGNC:10848" and e["object"] == "HGNC:20738":
            assert "aggregator_knowledge_source" in e
            assert "infores:string-database" in e["aggregator_knowledge_source"]
        if e["subject"] == "HGNC:10848" and e["object"] == "GO:0005576":
            assert "aggregator_knowledge_source" in e
            print("aggregator ks", e["aggregator_knowledge_source"])
        print(e)


def test_transform_knowledge_source_rewrite_with_prefix():
    """
    Transform graph from TSV to JSON.
    """
    inputs = [
        os.path.join(RESOURCE_DIR, "graph_tiny_nodes.tsv"),
        os.path.join(RESOURCE_DIR, "graph_tiny_edges.tsv"),
    ]
    output = os.path.join(TARGET_DIR, "graph.json")
    knowledge_sources = [
        ("aggregator_knowledge_source", "string,string database,new")
    ]
    transform(
        inputs=inputs,
        input_format="tsv",
        input_compression=None,
        output=output,
        output_format="json",
        output_compression=None,
        knowledge_sources=knowledge_sources,
    )
    assert os.path.exists(output)
    data = json.load(open(output, "r"))
    assert "nodes" in data
    assert "edges" in data
    assert len(data["nodes"]) == 6
    assert len(data["edges"]) == 9
    for e in data["edges"]:
        if e["subject"] == "HGNC:10848" and e["object"] == "HGNC:20738":
            assert "aggregator_knowledge_source" in e
            assert "infores:new-string-database" in e["aggregator_knowledge_source"]
            assert "biogrid" in e["aggregator_knowledge_source"]


def test_transform2():
    """
    Transform from a test transform YAML.
    """
    transform_config = os.path.join(RESOURCE_DIR, "test-transform.yaml")
    transform(inputs=None, transform_config=transform_config)
    assert os.path.exists(os.path.join(RESOURCE_DIR, "graph_nodes.tsv"))
    assert os.path.exists(os.path.join(RESOURCE_DIR, "graph_edges.tsv"))


def test_transform_rdf_to_tsv():
    """
    Transform from a test transform YAML.
    """
    transform_config = os.path.join(RESOURCE_DIR, "test-transform-rdf-tsv.yaml")
    transform(inputs=None, transform_config=transform_config)
    assert os.path.exists(os.path.join(TARGET_DIR, "test-transform-rdf_edges.tsv"))
    assert os.path.exists(os.path.join(TARGET_DIR, "test-transform-rdf_nodes.tsv"))


def test_transform_tsv_to_rdf():
    """
    Transform from a test transform YAML.
    """
    transform_config = os.path.join(RESOURCE_DIR, "test-transform-tsv-rdf.yaml")
    transform(inputs=None, transform_config=transform_config)
    assert os.path.exists(os.path.join(TARGET_DIR, "test-tranform-tsv-rdf.nt"))


def test_merge1():
    """
    Transform from test merge YAML.
    """
    merge_config = os.path.join(RESOURCE_DIR, "test-merge.yaml")
    merge(merge_config=merge_config)
    assert os.path.join(TARGET_DIR, "merged-graph_nodes.tsv")
    assert os.path.join(TARGET_DIR, "merged-graph_edges.tsv")
    assert os.path.join(TARGET_DIR, "merged-graph.json")


def test_merge2():
    """
    Transform selected source from test merge YAML and
    write selected destinations.
    """
    merge_config = os.path.join(RESOURCE_DIR, "test-merge.yaml")
    merge(merge_config=merge_config, destination=["merged-graph-json"])
    assert os.path.join(TARGET_DIR, "merged-graph.json")
