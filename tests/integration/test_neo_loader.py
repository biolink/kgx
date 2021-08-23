import os
import pytest

from kgx.transformer import Transformer
from tests import RESOURCE_DIR, TARGET_DIR
from tests.integration import (
    check_container,
    CONTAINER_NAME,
    DEFAULT_NEO4J_URL,
    DEFAULT_NEO4J_USERNAME,
    DEFAULT_NEO4J_PASSWORD,
)


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_csv_to_neo_load():
    """
    Test to load a CSV to Neo4j.
    """
    input_args1 = {
        "filename": [
            os.path.join(RESOURCE_DIR, "cm_nodes.csv"),
            os.path.join(RESOURCE_DIR, "cm_edges.csv"),
        ],
        "format": "csv",
    }
    t1 = Transformer()
    t1.transform(input_args1)

    output_args = {
        "uri": DEFAULT_NEO4J_URL,
        "username": DEFAULT_NEO4J_USERNAME,
        "password": DEFAULT_NEO4J_PASSWORD,
        "format": "neo4j",
    }
    t1.save(output_args)


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_neo_to_graph_transform():
    """
    Test to read from Neo4j and write to CSV.
    """
    input_args = {
        "uri": DEFAULT_NEO4J_URL,
        "username": DEFAULT_NEO4J_USERNAME,
        "password": DEFAULT_NEO4J_PASSWORD,
        "format": "neo4j",
    }
    output_filename = os.path.join(TARGET_DIR, "neo_graph")
    output_args = {"filename": output_filename, "format": "csv"}
    t = Transformer()
    t.transform(input_args, output_args)
    assert t.store.graph.number_of_nodes() == 10
    assert t.store.graph.number_of_edges() == 11
    assert os.path.exists(f"{output_filename}_nodes.csv")
    assert os.path.exists(f"{output_filename}_edges.csv")
