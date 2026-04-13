import os
import pytest

from kgx.transformer import Transformer
from tests import RESOURCE_DIR, TARGET_DIR
from kgx.config import get_logger

from tests.integration import (
    check_arango_container,
    clean_arango_slate,
    ARANGO_CONTAINER_NAME,
    DEFAULT_ARANGO_URL,
    DEFAULT_ARANGO_USERNAME,
    DEFAULT_ARANGO_PASSWORD,
    DEFAULT_ARANGO_DATABASE,
)

logger = get_logger()


@pytest.mark.skipif(
    not check_arango_container(),
    reason=f"Container {ARANGO_CONTAINER_NAME} is not running",
)
def test_csv_to_arango_to_csv_transform(clean_arango_slate):
    """
    Test to load a CSV KGX file into ArangoDB and then export back to CSV.
    """
    logger.debug("test_csv_to_arango_load...")
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
        "uri": DEFAULT_ARANGO_URL,
        "database": DEFAULT_ARANGO_DATABASE,
        "username": DEFAULT_ARANGO_USERNAME,
        "password": DEFAULT_ARANGO_PASSWORD,
        "format": "arangodb",
    }
    t1.save(output_args)

    """
    Continue sequentially to test read from ArangoDB to write out back to CSV.
    """
    logger.debug("test_arango_to_graph_transform")
    input_args = {
        "uri": DEFAULT_ARANGO_URL,
        "database": DEFAULT_ARANGO_DATABASE,
        "username": DEFAULT_ARANGO_USERNAME,
        "password": DEFAULT_ARANGO_PASSWORD,
        "format": "arangodb",
    }
    output_filename = os.path.join(TARGET_DIR, "arango_graph")
    output_args = {"filename": output_filename, "format": "csv"}
    t = Transformer()
    t.transform(input_args, output_args)
    assert t.store.graph.number_of_nodes() == 10
    assert t.store.graph.number_of_edges() == 11
    assert os.path.exists(f"{output_filename}_nodes.csv")
    assert os.path.exists(f"{output_filename}_edges.csv")


@pytest.mark.skipif(
    not check_arango_container(),
    reason=f"Container {ARANGO_CONTAINER_NAME} is not running",
)
def test_json_to_arango_to_csv_transform(clean_arango_slate):
    """
    Test to load a JSON KGX file into ArangoDB and then export to CSV.
    """
    logger.debug("test_json_to_arango_load...")
    input_args1 = {
        "filename": [os.path.join(RESOURCE_DIR, "json_edges.json")],
        "format": "json",
    }
    t1 = Transformer()
    t1.transform(input_args1)

    output_args = {
        "uri": DEFAULT_ARANGO_URL,
        "database": DEFAULT_ARANGO_DATABASE,
        "username": DEFAULT_ARANGO_USERNAME,
        "password": DEFAULT_ARANGO_PASSWORD,
        "format": "arangodb",
    }
    t1.save(output_args)

    """
    Continue sequentially to test read from ArangoDB to write out back to CSV.
    """
    logger.debug("test_arango_to_graph_transform")
    input_args = {
        "uri": DEFAULT_ARANGO_URL,
        "database": DEFAULT_ARANGO_DATABASE,
        "username": DEFAULT_ARANGO_USERNAME,
        "password": DEFAULT_ARANGO_PASSWORD,
        "format": "arangodb",
    }
    output_filename = os.path.join(TARGET_DIR, "arango_json_graph")
    output_args = {"filename": output_filename, "format": "csv"}
    t = Transformer()
    t.transform(input_args, output_args)

    assert os.path.exists(f"{output_filename}_nodes.csv")
    assert os.path.exists(f"{output_filename}_edges.csv")
