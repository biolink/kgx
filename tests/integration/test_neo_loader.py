import os
import pytest

from kgx.transformer import Transformer
from tests import RESOURCE_DIR, TARGET_DIR
from tests.unit import clean_database
from kgx.config import get_logger

from tests.integration import (
    check_container,
    CONTAINER_NAME,
    DEFAULT_NEO4J_URL,
    DEFAULT_NEO4J_USERNAME,
    DEFAULT_NEO4J_PASSWORD,
)

logger = get_logger()


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_csv_to_neo4j_load_to_graph_transform(clean_database):
    """
    Test to load a csv KGX file into Neo4j.
    """
    logger.debug("test_csv_to_neo4j_load...")
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

    """
    Continue sequentially to test read from Neo4j to write out back to CSV.
    """
    logger.debug("test_neo4j_to_graph_transform")
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


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_json_to_neo4j_load_to_graph_transform(clean_database):
    """
    Test to load a csv KGX file into Neo4j.
    """
    logger.debug("test_json_to_neo4j_load...")
    input_args1 = {
        "filename": [
            os.path.join(RESOURCE_DIR, "json_edges.json")
        ],
        "format": "json",
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

    """
    Continue sequentially to test read from Neo4j to write out back to CSV.
    """
    logger.debug("test_neo4j_to_graph_transform")
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

    assert os.path.exists(f"{output_filename}_nodes.csv")
    assert os.path.exists(f"{output_filename}_edges.csv")
