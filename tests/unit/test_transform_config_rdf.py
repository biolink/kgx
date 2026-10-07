"""
Transform-config driven RDF export: property exclusion, sink prefix expansion,
explicit predicate mappings, and parallel export through the config path.
"""
import os

import pytest
import yaml

try:
    import duckdb

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

from kgx.cli.cli_utils import prepare_output_args, prepare_top_level_args, transform
from kgx.transformer import Transformer

pytestmark = pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")

REACTOME_IRI = "https://reactome.org/content/detail/"
CUSTOM_IRI = "https://example.org/custom/score"


@pytest.fixture
def kg_duckdb(tmp_path):
    """A small graph shaped like a koza-merged KG, bookkeeping columns included."""
    db_path = str(tmp_path / "kg.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute(
        "CREATE TABLE nodes (id VARCHAR, category VARCHAR, name VARCHAR, "
        "file_source VARCHAR, namespace VARCHAR, custom_score VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE edges (id VARCHAR, subject VARCHAR, predicate VARCHAR, object VARCHAR, "
        "category VARCHAR, file_source VARCHAR, grouping_key VARCHAR)"
    )
    nodes = [
        (f"Reactome:R-HSA-{i}", "biolink:Pathway", f"Pathway {i}", "reactome_nodes", "Reactome", "0.5")
        for i in range(20)
    ]
    conn.executemany("INSERT INTO nodes VALUES (?, ?, ?, ?, ?, ?)", nodes)
    edges = [
        (
            f"urn:uuid:00000000-0000-0000-0000-{i:012d}",
            f"Reactome:R-HSA-{i}",
            "biolink:related_to",
            f"Reactome:R-HSA-{(i + 1) % 20}",
            "biolink:Association",
            "reactome_edges",
            f"key-{i}",
        )
        for i in range(30)
    ]
    conn.executemany("INSERT INTO edges VALUES (?, ?, ?, ?, ?, ?, ?)", edges)
    conn.close()
    return db_path


def _write_config(tmp_path, db_path, output_name, configuration):
    cfg = {
        "configuration": {"output_directory": str(tmp_path), **configuration},
        "transform": {
            "source": {
                "kg": {
                    "input": {"format": "duckdb", "filename": [db_path]},
                    "output": {"format": "nt", "filename": output_name},
                }
            }
        },
    }
    path = tmp_path / f"{output_name}.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return str(path)


CONFIGURATION = {
    "exclude_node_properties": ["file_source", "namespace"],
    "exclude_edge_properties": ["file_source", "grouping_key"],
    "prefix_map": {"Reactome": REACTOME_IRI},
    "reverse_predicate_mappings": {"custom_score": CUSTOM_IRI},
}


def _run(tmp_path, kg_duckdb, name, parallel=1):
    config = _write_config(tmp_path, kg_duckdb, f"{name}.nt", CONFIGURATION)
    transform(inputs=None, transform_config=config, stream=True, parallel=parallel)
    with open(tmp_path / f"{name}.nt") as fh:
        return sorted(line for line in fh if line.strip())


def test_prepare_top_level_args_reads_exclusions():
    args = prepare_top_level_args(
        {"exclude_node_properties": ["a"], "exclude_edge_properties": ["b", "c"]}
    )
    assert args["exclude_node_properties"] == ["a"]
    assert args["exclude_edge_properties"] == ["b", "c"]
    assert prepare_top_level_args({})["exclude_node_properties"] == []


def test_prepare_output_args_merges_source_settings():
    source = {
        "output": {"format": "nt", "filename": "out.nt"},
        "prefix_map": {"B": "https://b.example.org/"},
        "exclude_edge_properties": ["grouping_key"],
    }
    output_args = prepare_output_args(
        "kg",
        source,
        None,
        reverse_predicate_mappings={"custom_score": CUSTOM_IRI},
        prefix_map={"A": "https://a.example.org/"},
        exclude_node_properties=["file_source"],
        exclude_edge_properties=["file_source"],
    )
    assert output_args["prefix_map"] == {
        "A": "https://a.example.org/",
        "B": "https://b.example.org/",
    }
    # the transformer reads the singular key
    assert output_args["reverse_predicate_mapping"] == {"custom_score": CUSTOM_IRI}
    assert output_args["exclude_node_properties"] == ["file_source"]
    assert output_args["exclude_edge_properties"] == ["file_source", "grouping_key"]


def test_config_excludes_properties_and_expands_prefixes(tmp_path, kg_duckdb):
    lines = _run(tmp_path, kg_duckdb, "sequential")
    text = "".join(lines)
    for excluded in ("file_source", "namespace", "grouping_key"):
        assert excluded not in text
    assert f"<{REACTOME_IRI}R-HSA-1>" in text
    assert "example.org/UNKNOWN/Reactome" not in text
    assert f"<{CUSTOM_IRI}>" in text


def test_config_parallel_matches_sequential(tmp_path, kg_duckdb):
    sequential = _run(tmp_path, kg_duckdb, "sequential")
    parallel = _run(tmp_path, kg_duckdb, "parallel", parallel=3)
    assert parallel == sequential
    assert len(parallel) > 0


def test_explicit_mapping_overrides_biolink_slot(tmp_path):
    """`name` resolves to rdfs:label in Biolink; an explicit mapping must win."""
    from kgx.sink import RdfSink

    out = str(tmp_path / "mapped.nt")
    sink = RdfSink(Transformer(), filename=out, format="nt")
    sink.set_reverse_predicate_mapping({"name": CUSTOM_IRI})
    sink.write_node({"id": "HGNC:1", "category": ["biolink:Gene"], "name": "A1BG"})
    sink.finalize()
    text = open(out).read()
    assert f"<{CUSTOM_IRI}>" in text
    assert "rdf-schema#label" not in text


def test_reverse_predicate_mapping_accepts_inverted_form(tmp_path):
    from kgx.sink import RdfSink

    sink = RdfSink(Transformer(), filename=str(tmp_path / "x.nt"), format="nt")
    sink.set_reverse_predicate_mapping({CUSTOM_IRI: "custom_score"})
    assert str(sink.reverse_predicate_mapping["custom_score"]) == CUSTOM_IRI
    sink.finalize()


def test_required_properties_are_never_excluded():
    from unittest import mock

    import kgx.transformer as transformer_module

    t = Transformer()
    output_args = {"exclude_edge_properties": ["subject", "grouping_key"]}
    with mock.patch.object(transformer_module.log, "warning") as warning:
        t._set_property_exclusions(output_args)
    assert t.exclude_edge_properties == {"grouping_key"}
    assert "exclude_edge_properties" not in output_args
    warning.assert_called_once()
    assert "subject" in warning.call_args.args[0]
