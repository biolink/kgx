"""
Tests for Transformer parallel mode (multiprocessing-based partitioned export).
Currently exercises the only supported combo: DuckDB source + N-Triples sink.
"""
import os
import tempfile

import pytest

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

from kgx.transformer import Transformer


@pytest.fixture
def populated_duckdb():
    """Create a DuckDB file with enough rows to give every worker a chunk."""
    if not DUCKDB_AVAILABLE:
        pytest.skip("DuckDB not available")

    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=True) as tmp:
        db_path = tmp.name

    conn = duckdb.connect(db_path)
    conn.execute(
        "CREATE TABLE nodes (id VARCHAR PRIMARY KEY, category VARCHAR, name VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE edges (id VARCHAR, subject VARCHAR, predicate VARCHAR, object VARCHAR)"
    )

    n_nodes = 40
    n_edges = 60
    conn.executemany(
        "INSERT INTO nodes VALUES (?, ?, ?)",
        [(f"CURIE:{i:04d}", "biolink:Gene", f"Gene {i}") for i in range(n_nodes)],
    )
    conn.executemany(
        "INSERT INTO edges VALUES (?, ?, ?, ?)",
        [
            (
                f"e{i:04d}",
                f"CURIE:{i % n_nodes:04d}",
                "biolink:related_to",
                f"CURIE:{(i + 1) % n_nodes:04d}",
            )
            for i in range(n_edges)
        ],
    )
    conn.close()

    yield db_path

    if os.path.exists(db_path):
        os.unlink(db_path)


def _read_sorted_lines(path):
    with open(path, "rb") as f:
        return sorted(line for line in f if line.strip())


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_partitions_cover_full_range(populated_duckdb):
    """partitions() must yield disjoint ranges that sum to the full row count."""
    from kgx.source import DuckDbSource

    t = Transformer()
    src = DuckDbSource(t)
    parts = src.partitions(populated_duckdb, 4)

    assert len(parts) == 4
    # Disjoint, contiguous, fully cover.
    node_total = sum(p["node_end"] - p["node_start"] for p in parts)
    edge_total = sum(p["edge_end"] - p["edge_start"] for p in parts)
    assert node_total == 40
    assert edge_total == 60
    # First partition starts at zero; last ends at total.
    assert parts[0]["node_start"] == 0 and parts[0]["edge_start"] == 0
    assert parts[-1]["node_end"] == 40 and parts[-1]["edge_end"] == 60


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_parallel_matches_sequential(populated_duckdb):
    """parallel=4 must produce the same N-Triples set as parallel=1."""
    seq_path = tempfile.mktemp(suffix=".nt")
    par_path = tempfile.mktemp(suffix=".nt")

    try:
        Transformer(stream=True).transform(
            input_args={"filename": [populated_duckdb], "format": "duckdb"},
            output_args={"filename": seq_path, "format": "nt"},
        )
        Transformer(stream=True).transform(
            input_args={"filename": [populated_duckdb], "format": "duckdb"},
            output_args={"filename": par_path, "format": "nt"},
            parallel=4,
        )

        seq_lines = _read_sorted_lines(seq_path)
        par_lines = _read_sorted_lines(par_path)
        assert seq_lines == par_lines
        assert len(par_lines) > 0
    finally:
        for p in (seq_path, par_path):
            if os.path.exists(p):
                os.unlink(p)


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not available")
def test_parallel_unsupported_format_falls_back(populated_duckdb, caplog):
    """Unsupported sink format should warn and run sequentially, not raise."""
    out_path = tempfile.mktemp(suffix=".tsv")
    nodes_path = out_path.replace(".tsv", "_nodes.tsv")
    edges_path = out_path.replace(".tsv", "_edges.tsv")
    try:
        Transformer(stream=True).transform(
            input_args={"filename": [populated_duckdb], "format": "duckdb"},
            output_args={
                "filename": out_path.replace(".tsv", ""),
                "format": "tsv",
                "node_properties": ["id", "category", "name"],
                "edge_properties": ["id", "subject", "predicate", "object"],
            },
            parallel=4,
        )
        # Sequential fallback should still produce TSV outputs.
        assert os.path.exists(nodes_path)
        assert os.path.exists(edges_path)
    finally:
        for p in (nodes_path, edges_path):
            if os.path.exists(p):
                os.unlink(p)
