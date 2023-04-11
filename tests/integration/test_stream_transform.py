import copy
import os
import pytest
from neo4j import GraphDatabase
import neo4j
from kgx.transformer import Transformer
from tests import TARGET_DIR, RESOURCE_DIR

from tests.integration import (
    check_container,
    CONTAINER_NAME,
    DEFAULT_NEO4J_URL,
    DEFAULT_NEO4J_USERNAME,
    DEFAULT_NEO4J_PASSWORD,
    clean_slate
)


def clean_database():
    driver = GraphDatabase.driver(
        DEFAULT_NEO4J_URL, auth=(DEFAULT_NEO4J_USERNAME, DEFAULT_NEO4J_PASSWORD)
    )
    session = driver.session()

    q = "MATCH (n) DETACH DELETE n"
    with session.begin_transaction() as tx:
        tx.run(q)
        tx.commit()
        tx.close()


def run_transform(query):
    clean_database()
    _transform(copy.deepcopy(query))


def _transform(query):
    """
    Transform an input to an output via Transformer.
    """
    t1 = Transformer()
    t1.transform(query[0])
    t1.save(query[1].copy())

    print("query[0]", query[0])
    print("number of nodes: ", t1.store.graph.number_of_nodes(), "expected: ", query[2])
    print("number of edges: ", t1.store.graph.number_of_edges(), "expected: ", query[3])

    assert t1.store.graph.number_of_nodes() == query[2]
    assert t1.store.graph.number_of_edges() == query[3]

    output = query[1]
    if output["format"] in {"tsv", "csv", "jsonl"}:
        input_args = {
            "filename": [
                f"{output['filename']}_nodes.{output['format']}",
                f"{output['filename']}_edges.{output['format']}",
            ],
            "format": output["format"],
        }
    elif output["format"] in {"neo4j"}:
        input_args = {
            "uri": DEFAULT_NEO4J_URL,
            "username": DEFAULT_NEO4J_USERNAME,
            "password": DEFAULT_NEO4J_PASSWORD,
            "format": "neo4j",
        }
    else:
        input_args = {"filename": [f"{output['filename']}"], "format": output["format"]}

    t2 = Transformer()
    t2.transform(input_args)

    print("query[0]", query[0])
    print("number of nodes: ", t2.store.graph.number_of_nodes(), "expected: ", query[4])
    print("number of edges: ", t2.store.graph.number_of_edges(), "expected: ", query[5])
    assert t2.store.graph.number_of_nodes() == query[4]
    assert t2.store.graph.number_of_edges() == query[5]


def _stream_transform(query):
    """
    Transform an input to an output via Transformer where streaming is enabled.
    """
    t1 = Transformer(stream=True)
    t1.transform(query[0], query[1])

    output = query[1]
    if output["format"] in {"tsv", "csv", "jsonl"}:
        input_args = {
            "filename": [
                f"{output['filename']}_nodes.{output['format']}",
                f"{output['filename']}_edges.{output['format']}",
            ],
            "format": output["format"],
        }
    elif output["format"] in {"neo4j"}:
        input_args = {
            "uri": DEFAULT_NEO4J_URL,
            "username": DEFAULT_NEO4J_USERNAME,
            "password": DEFAULT_NEO4J_PASSWORD,
            "format": "neo4j",
        }
    else:
        input_args = {"filename": [f"{output['filename']}"], "format": output["format"]}

    t2 = Transformer()
    t2.transform(input_args)

    print("query[0]",query[0])
    print("number of nodes: ", t2.store.graph.number_of_nodes(), "expected: ", query[2])
    print("number of edges: ", t2.store.graph.number_of_edges(), "expected: ", query[3])
    assert t2.store.graph.number_of_nodes() == query[2]
    assert t2.store.graph.number_of_edges() == query[3]


@pytest.mark.parametrize(
    "query",
    [
        (
            {
                "filename": [
                    os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
                    os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
                ],
                "format": "tsv",
            },
            {"filename": os.path.join(TARGET_DIR, "graph1.json"), "format": "json"},
            512,
            531,
            512,
            531,
        ),
        (
            {
                "filename": [
                    os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
                    os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
                ],
                "format": "tsv",
            },
            {"filename": os.path.join(TARGET_DIR, "graph2"), "format": "jsonl"},
            512,
            531,
            512,
            531,
        ),
        (
            {
                "filename": [
                    os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
                    os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
                ],
                "format": "tsv",
                "lineterminator": None,
            },
            {"filename": os.path.join(TARGET_DIR, "graph3.nt"), "format": "nt"},
            512,
            531,
            512,
            531,
        ),
        (
            {
                "filename": [
                    os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
                    os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
                ],
                "format": "tsv",
                "node_filters": {"category": {"biolink:Gene"}},
            },
            {"filename": os.path.join(TARGET_DIR, "graph4"), "format": "jsonl"},
            178,
            177,
            178,
            177,
        ),
        (
            {
                "filename": [
                    os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
                    os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
                ],
                "format": "tsv",
                "node_filters": {"category": {"biolink:Gene"}},
                "edge_filters": {"predicate": {"biolink:interacts_with"}},
            },
            {"filename": os.path.join(TARGET_DIR, "graph5"), "format": "jsonl"},
            178,
            165,
            178,
            165,
        ),
        (
            {
                "filename": [
                    os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
                    os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
                ],
                "format": "tsv",
                "edge_filters": {
                    "subject_category": {"biolink:Disease"},
                    "object_category": {"biolink:PhenotypicFeature"},
                    "predicate": {"biolink:has_phenotype"},
                },
            },
            {"filename": os.path.join(TARGET_DIR, "graph6"), "format": "jsonl"},
            133,
            13,
            133,
            13,
        ),
    ],
)
def test_transform1(query):
    """
    Test loading data from a TSV source and writing to various sinks.
    """
    run_transform(query)


@pytest.mark.parametrize(
    "query",
    [
        (
            {"filename": [os.path.join(RESOURCE_DIR, "graph.json")], "format": "json"},
            {
                "filename": os.path.join(TARGET_DIR, "graph1s2"),
                "format": "tsv",
                "node_properties": ["id", "name", "category", "taxon"],
                "edge_properties": [
                    "subject",
                    "predicate",
                    "object",
                    "relation",
                    "knowledge_source",
                ],
            },
            512,
            532,
            512,
            532,

        ),
        (
            {"filename": [os.path.join(RESOURCE_DIR, "graph.json")], "format": "json"},
            {"filename": os.path.join(TARGET_DIR, "graph2s2"), "format": "jsonl"},
            512,
            532,
            512,
            532,
        ),
        (
            {"filename": [os.path.join(RESOURCE_DIR, "graph.json")], "format": "json"},
            {"filename": os.path.join(TARGET_DIR, "graph3s2.nt"), "format": "nt"},
            512,
            532,
            512,
            532,
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "graph.json")],
                "format": "json",
                "edge_filters": {
                    "subject_category": {"biolink:Disease"},
                    "object_category": {"biolink:PhenotypicFeature"},
                    "predicate": {"biolink:has_phenotype"},
                },
            },
            {"filename": os.path.join(TARGET_DIR, "graph4s2"), "format": "jsonl"},
            133,
            13,
            133,
            13,
        ),
    ],
)
def test_transform2(query):
    """
    Test loading data from JSON source and writing to various sinks.
    """
    run_transform(query)


@pytest.mark.parametrize(
    "query",
    [
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "rdf", "test3.nt")],
                "format": "nt",
            },
            {
                "filename": os.path.join(TARGET_DIR, "graph1s3"),
                "format": "tsv",
                "node_properties": [
                    "id",
                    "name",
                    "category",
                    "description",
                    "knowledge_source",
                ],
                "edge_properties": [
                    "subject",
                    "predicate",
                    "object",
                    "relation",
                    "category",
                    "fusion",
                    "homology",
                    "combined_score",
                    "cooccurrence",
                ],
            },
            7,
            6,
            7,
            6,
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "rdf", "test3.nt")],
                "format": "nt",
            },
            {"filename": os.path.join(TARGET_DIR, "graph2s3.json"), "format": "json"},
            7,
            6,
            7,
            6,
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "rdf", "test3.nt")],
                "format": "nt",
            },
            {"filename": os.path.join(TARGET_DIR, "graph3s3"), "format": "jsonl"},
            7,
            6,
            7,
            6,
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "rdf", "test3.nt")],
                "format": "nt",
                "edge_filters": {
                    "subject_category": {"biolink:Gene", "biolink:Protein"},
                    "object_category": {"biolink:Gene", "biolink:Protein"},
                    "predicate": {"biolink:has_gene_product", "biolink:interacts_with"},
                },
            },
            {"filename": os.path.join(TARGET_DIR, "graph4s3"), "format": "jsonl"},
            6,
            3,
            6,
            3,
        ),
    ],
)
def test_transform3(query):
    """
    Test loading data from RDF source and writing to various sinks.
    """
    run_transform(query)


@pytest.mark.parametrize(
    "query",
    [
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "goslim_generic.json")],
                "format": "obojson",
            },
            {
                "filename": os.path.join(TARGET_DIR, "graph1s4"),
                "format": "tsv",
                "node_properties": [
                    "id",
                    "name",
                    "category",
                    "description",
                    "knowledge_source",
                ],
                "edge_properties": [
                    "subject",
                    "predicate",
                    "object",
                    "relation",
                    "category",
                ],
            },
            176,
            205,
            176,
            205,
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "goslim_generic.json")],
                "format": "obojson",
            },
            {
                "filename": os.path.join(TARGET_DIR, "graph2s4"),
                "format": "jsonl",
            },
            176,
            205,
            176,
            205,
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "goslim_generic.json")],
                "format": "obojson",
            },
            {
                "filename": os.path.join(TARGET_DIR, "graph3s4.nt"),
                "format": "nt",
            },
            176,
            205,
            176,
            205,
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "goslim_generic.json")],
                "format": "obojson",
                "edge_filters": {
                    "subject_category": {"biolink:BiologicalProcess"},
                    "predicate": {"biolink:subclass_of"},
                },
            },
            {"filename": os.path.join(TARGET_DIR, "graph4s4"), "format": "jsonl"},
            72,
            73,
            72,
            73,
        ),
    ],
)
def test_transform4(query):
    """
    Test loading data from RDF source and writing to various sinks.
    """
    run_transform(query)


@pytest.mark.parametrize(
    "query",
    [
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "goslim_generic.owl")],
                "format": "owl",
            },
            {
                "filename": os.path.join(TARGET_DIR, "graph1s5"),
                "format": "tsv",
                "node_properties": [
                    "id",
                    "name",
                    "category",
                    "description",
                    "knowledge_source",
                ],
                "edge_properties": [
                    "subject",
                    "predicate",
                    "object",
                    "relation",
                    "category",
                ],
            },
            220,
            1050,
            220,
            1050,
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "goslim_generic.owl")],
                "format": "owl",
            },
            {"filename": os.path.join(TARGET_DIR, "graph2s5"), "format": "jsonl"},
            220,
            1050,
            220,
            1050,
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "goslim_generic.owl")],
                "format": "owl",
            },
            {"filename": os.path.join(TARGET_DIR, "graph3s5.nt"), "format": "nt"},
            220,
            1050,
            221,
            1052,
        ),
    ],
)
def test_transform5(query):
    """
    Test transforming data from an OWL source and writing to various sinks.
    """
    run_transform(query)


@pytest.mark.parametrize(
    "query",
    [
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "rsa_sample.json")],
                "format": "trapi-json",
            },
            {
                "filename": os.path.join(TARGET_DIR, "graph1s6"),
                "format": "tsv",
                "node_properties": [
                    "id",
                    "name",
                    "category",
                    "description",
                    "knowledge_source",
                ],
                "edge_properties": [
                    "subject",
                    "predicate",
                    "object",
                    "relation",
                    "category",
                ],
            },
            4,
            3,
            4,
            3,
        ),
        (
                {
                    "filename": [os.path.join(RESOURCE_DIR, "rsa_sample.json")],
                    "format": "trapi-json",
                },
                {
                    "filename": os.path.join(TARGET_DIR, "graph2s6.json"),
                    "format": "json",
                },
                4,
                3,
                4,
                3,
        ),
        (
                {
                    "filename": [os.path.join(RESOURCE_DIR, "rsa_sample.json")],
                    "format": "trapi-json",
                },
                {
                    "filename": os.path.join(TARGET_DIR, "graph3s6"),
                    "format": "jsonl",
                },
                4,
                3,
                4,
                3,
        ),
        (
                {
                    "filename": [os.path.join(RESOURCE_DIR, "rsa_sample.json")],
                    "format": "trapi-json",
                },
                {
                    "filename": os.path.join(TARGET_DIR, "graph4s6.nt"),
                    "format": "nt",
                },
                4,
                3,
                4,
                3,
        ),
        (
                {
                    "filename": [os.path.join(RESOURCE_DIR, "rsa_sample.json")],
                    "format": "trapi-json",
                    "edge_filters": {
                        "subject_category": {"biolink:Disease"},
                    },
                },
                {"filename": os.path.join(TARGET_DIR, "graph5s6"), "format": "jsonl"},
                2,
                0,
                2,
                0,
        ),
    ],
)
def test_transform6(query):
    """
    Test transforming data from RDF source and writing to various sinks.
    """
    run_transform(query)


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_transform7():
    """
    Test transforming data from various sources to a Neo4j sink.
    """
    clean_database()
    t1 = Transformer()
    t1.transform(input_args={
        "filename": [
            os.path.join(RESOURCE_DIR, "graph_nodes.tsv"),
            os.path.join(RESOURCE_DIR, "graph_edges.tsv"),
        ],
        "format": "tsv",
    },
        output_args={
            "uri": DEFAULT_NEO4J_URL,
            "username": DEFAULT_NEO4J_USERNAME,
            "password": DEFAULT_NEO4J_PASSWORD,
            "format": "neo4j",
        })

    assert t1.store.graph.number_of_nodes() == 512
    assert t1.store.graph.number_of_edges() == 531


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_transform8():
    clean_database()
    t1 = Transformer()
    t1.transform(input_args={"filename": [os.path.join(RESOURCE_DIR, "graph.json")], "format": "json"},
                 output_args={
                     "uri": DEFAULT_NEO4J_URL,
                     "username": DEFAULT_NEO4J_USERNAME,
                     "password": DEFAULT_NEO4J_PASSWORD,
                     "format": "neo4j",
                 })

    assert t1.store.graph.number_of_nodes() == 512
    assert t1.store.graph.number_of_edges() == 532


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_transform9():
    clean_database()
    t1 = Transformer()
    t1.transform(input_args={
        "filename": [os.path.join(RESOURCE_DIR, "rdf", "test3.nt")],
        "format": "nt",
    },
        output_args={
            "uri": DEFAULT_NEO4J_URL,
            "username": DEFAULT_NEO4J_USERNAME,
            "password": DEFAULT_NEO4J_PASSWORD,
            "format": "neo4j",
        })

    assert t1.store.graph.number_of_nodes() == 7
    assert t1.store.graph.number_of_edges() == 6


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_transform10():
    clean_database()
    t1 = Transformer()
    t1.transform(input_args={
        "filename": [os.path.join(RESOURCE_DIR, "goslim_generic.json")],
        "format": "obojson",
    },
        output_args={
            "uri": DEFAULT_NEO4J_URL,
            "username": DEFAULT_NEO4J_USERNAME,
            "password": DEFAULT_NEO4J_PASSWORD,
            "format": "neo4j",
        })

    assert t1.store.graph.number_of_nodes() == 176
    assert t1.store.graph.number_of_edges() == 205


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_transform11():
    clean_database()
    t1 = Transformer()
    t1.transform(input_args={
        "filename": [os.path.join(RESOURCE_DIR, "goslim_generic.owl")],
        "format": "owl",
    },
        output_args={
            "uri": DEFAULT_NEO4J_URL,
            "username": DEFAULT_NEO4J_USERNAME,
            "password": DEFAULT_NEO4J_PASSWORD,
            "format": "neo4j",
        })

    assert t1.store.graph.number_of_nodes() == 220
    assert t1.store.graph.number_of_edges() == 1050


@pytest.mark.skipif(
    not check_container(), reason=f"Container {CONTAINER_NAME} is not running"
)
def test_transform12():
    clean_database()
    t1 = Transformer()
    t1.transform(input_args={
        "filename": [os.path.join(RESOURCE_DIR, "rsa_sample.json")],
        "format": "trapi-json",
    },
        output_args={
            "uri": DEFAULT_NEO4J_URL,
            "username": DEFAULT_NEO4J_USERNAME,
            "password": DEFAULT_NEO4J_PASSWORD,
            "format": "neo4j",
        })

    assert t1.store.graph.number_of_nodes() == 4
    assert t1.store.graph.number_of_edges() == 3
