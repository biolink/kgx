import copy
import os
import pytest

from kgx.transformer import Transformer
from tests import TARGET_DIR, RESOURCE_DIR, print_graph
from tests.integration import (
    clean_slate,
    DEFAULT_NEO4J_URL,
    DEFAULT_NEO4J_USERNAME,
    DEFAULT_NEO4J_PASSWORD,
)


def run_transform(query):
    _transform(copy.deepcopy(query))
    _stream_transform(copy.deepcopy(query))


def _transform(query):
    """
    Transform an input to an output via Transformer.
    """
    t1 = Transformer()
    t1.transform(query[0])
    t1.save(query[1].copy())

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

    assert t2.store.graph.number_of_nodes() == query[2]
    assert t2.store.graph.number_of_edges() == query[3]


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
        ),
        (
            {"filename": [os.path.join(RESOURCE_DIR, "graph.json")], "format": "json"},
            {"filename": os.path.join(TARGET_DIR, "graph2s2"), "format": "jsonl"},
            512,
            532,
        ),
        (
            {"filename": [os.path.join(RESOURCE_DIR, "graph.json")], "format": "json"},
            {"filename": os.path.join(TARGET_DIR, "graph3s2.nt"), "format": "nt"},
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
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "rdf", "test3.nt")],
                "format": "nt",
            },
            {"filename": os.path.join(TARGET_DIR, "graph2s3.json"), "format": "json"},
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
            206,
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
            206,
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
            206,
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
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "goslim_generic.owl")],
                "format": "owl",
            },
            {"filename": os.path.join(TARGET_DIR, "graph2s5"), "format": "jsonl"},
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
        ),
        # (
        #     {
        #         'filename': [os.path.join(RESOURCE_DIR, 'goslim_generic.owl')],
        #         'format': 'owl',
        #         'edge_filters': {
        #             'subject_category': {'biolink:BiologicalProcess'},
        #             'predicate': {'biolink:subclass_of'}
        #         }
        #     },
        #     {
        #         'filename': os.path.join(TARGET_DIR, 'graph4s5'),
        #         'format': 'jsonl'
        #     },
        #     220,
        #     1050
        # )
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
        ),
    ],
)
def test_transform6(query):
    """
    Test transforming data from RDF source and writing to various sinks.
    """
    run_transform(query)


@pytest.mark.skip()
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
            {
                "uri": DEFAULT_NEO4J_URL,
                "username": DEFAULT_NEO4J_USERNAME,
                "password": DEFAULT_NEO4J_PASSWORD,
                "format": "neo4j",
            },
            512,
            531,
        ),
        (
            {"filename": [os.path.join(RESOURCE_DIR, "graph.json")], "format": "json"},
            {
                "uri": DEFAULT_NEO4J_URL,
                "username": DEFAULT_NEO4J_USERNAME,
                "password": DEFAULT_NEO4J_PASSWORD,
                "format": "neo4j",
            },
            512,
            531,
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "rdf", "test3.nt")],
                "format": "nt",
            },
            {
                "uri": DEFAULT_NEO4J_URL,
                "username": DEFAULT_NEO4J_USERNAME,
                "password": DEFAULT_NEO4J_PASSWORD,
                "format": "neo4j",
            },
            7,
            6,
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "goslim_generic.json")],
                "format": "obojson",
            },
            {
                "uri": DEFAULT_NEO4J_URL,
                "username": DEFAULT_NEO4J_USERNAME,
                "password": DEFAULT_NEO4J_PASSWORD,
                "format": "neo4j",
            },
            176,
            206,
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "goslim_generic.owl")],
                "format": "owl",
            },
            {
                "uri": DEFAULT_NEO4J_URL,
                "username": DEFAULT_NEO4J_USERNAME,
                "password": DEFAULT_NEO4J_PASSWORD,
                "format": "neo4j",
            },
            220,
            1050,
        ),
        (
            {
                "filename": [os.path.join(RESOURCE_DIR, "rsa_sample.json")],
                "format": "trapi-json",
            },
            {
                "uri": DEFAULT_NEO4J_URL,
                "username": DEFAULT_NEO4J_USERNAME,
                "password": DEFAULT_NEO4J_PASSWORD,
                "format": "neo4j",
            },
            4,
            3,
        ),
    ],
)
def test_transform7(clean_slate, query):
    """
    Test transforming data from various sources to a Neo4j sink.
    """
    run_transform(query)
