import csv
import os

import pytest

from kgx.cli.cli_utils import arango_download
from kgx.source import ArangoSource
from kgx.transformer import Transformer
from tests import TARGET_DIR
from tests.unit import (
    clean_arango_database,
    DEFAULT_ARANGO_URL,
    DEFAULT_ARANGO_USERNAME,
    DEFAULT_ARANGO_PASSWORD,
    DEFAULT_ARANGO_DATABASE,
    load_graph_dictionary,
    check_arango_container,
    ARANGO_CONTAINER_NAME,
)


@pytest.fixture(scope="function")
def ontology_collections():
    """
    Create per-ontology collections holding a graph in which a stored 'id'
    disagrees with the document handle, as annotation-derived ids can:
    CL/0020036 and CL/0020041 both store 'CL:9900001'.  CL/0020036 has two
    edges to CL/0020041 in the same collection that differ only by Label.
    Yields the database; drops the collections afterwards.
    """
    from arango import ArangoClient

    client = ArangoClient(hosts=DEFAULT_ARANGO_URL)
    db = client.db(
        DEFAULT_ARANGO_DATABASE,
        username=DEFAULT_ARANGO_USERNAME,
        password=DEFAULT_ARANGO_PASSWORD,
    )
    names = {"CL": False, "UBERON": False, "CL-CL": True, "CL-UBERON": True}
    for name, edge in names.items():
        if db.has_collection(name):
            db.delete_collection(name)
        db.create_collection(name, edge=edge)

    db.collection("CL").insert_many(
        [
            {"_key": "0020036", "id": "CL:9900001", "label": "oRGC1"},
            {"_key": "0020041", "id": "CL:9900001", "label": "oRGC4"},
            {"_key": "1000300", "id": "CL:1000300", "label": "kidney cell"},
        ]
    )
    db.collection("UBERON").insert({"_key": "0001992", "label": "kidney"})
    db.collection("CL-CL").insert_many(
        [
            {"_from": "CL/0020036", "_to": "CL/0020041", "Label": "SYNAPSED_BY"},
            {"_from": "CL/0020036", "_to": "CL/0020041", "Label": "SYNAPSED_TO"},
        ]
    )
    db.collection("CL-UBERON").insert(
        {"_from": "CL/0020041", "_to": "UBERON/0001992", "Label": "PART_OF"}
    )
    try:
        yield db
    finally:
        for name in names:
            if db.has_collection(name):
                db.delete_collection(name)


def _read_tsv(filename):
    with open(filename, newline="") as f:
        return list(csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE))


@pytest.mark.skipif(
    not check_arango_container(),
    reason=f"Container {ARANGO_CONTAINER_NAME} is not running",
)
def test_read_arango_curie_convention(clean_arango_database):
    """
    Read a graph from per-ontology collections using the CURIE convention.
    Documents without a stored 'id' field should have their id reconstructed
    from the collection name and _key (e.g., collection "CL", _key "1000300"
    yields id "CL:1000300").
    """
    from arango import ArangoClient

    client = ArangoClient(hosts=DEFAULT_ARANGO_URL)
    db = client.db(
        DEFAULT_ARANGO_DATABASE,
        username=DEFAULT_ARANGO_USERNAME,
        password=DEFAULT_ARANGO_PASSWORD,
    )

    # Create per-ontology collections
    if not db.has_collection("CL"):
        db.create_collection("CL")
    if not db.has_collection("UBERON"):
        db.create_collection("UBERON")
    if not db.has_collection("CL-UBERON"):
        db.create_collection("CL-UBERON", edge=True)

    cl_col = db.collection("CL")
    uberon_col = db.collection("UBERON")
    edge_col = db.collection("CL-UBERON")

    # Insert nodes without 'id' field — id should be reconstructed from collection:_key
    cl_col.insert({"_key": "1000300", "name": "kidney cell", "category": ["biolink:Cell"]})
    uberon_col.insert({"_key": "0001992", "name": "kidney", "category": ["biolink:GrossAnatomicalStructure"]})

    # Insert edge without 'id' field
    edge_col.insert({
        "_from": "CL/1000300",
        "_to": "UBERON/0001992",
        "predicate": "biolink:located_in",
        "relation": "RO:0001025",
    })

    t = Transformer()
    s = ArangoSource(t)

    g = s.parse(
        uri=DEFAULT_ARANGO_URL,
        database=DEFAULT_ARANGO_DATABASE,
        username=DEFAULT_ARANGO_USERNAME,
        password=DEFAULT_ARANGO_PASSWORD,
        node_collections=["CL", "UBERON"],
        edge_collections=["CL-UBERON"],
    )

    nodes, edges = load_graph_dictionary(g)

    # IDs should be reconstructed as collection:_key
    assert "CL:1000300" in nodes
    assert "UBERON:0001992" in nodes
    assert nodes["CL:1000300"]["name"] == "kidney cell"

    edge = edges[("CL:1000300", "UBERON:0001992")][0]
    assert edge["predicate"] == "biolink:located_in"

    # Cleanup per-ontology collections (clean_arango_database only drops nodes/edges)
    for col in ["CL", "UBERON", "CL-UBERON"]:
        if db.has_collection(col):
            db.delete_collection(col)


@pytest.mark.skipif(
    not check_arango_container(),
    reason=f"Container {ARANGO_CONTAINER_NAME} is not running",
)
def test_read_arango(clean_arango_database):
    """
    Read a graph from an ArangoDB instance.
    """
    from arango import ArangoClient

    client = ArangoClient(hosts=DEFAULT_ARANGO_URL)
    db = client.db(
        DEFAULT_ARANGO_DATABASE,
        username=DEFAULT_ARANGO_USERNAME,
        password=DEFAULT_ARANGO_PASSWORD,
    )

    # Create collections
    if not db.has_collection("nodes"):
        db.create_collection("nodes")
    if not db.has_collection("edges"):
        db.create_collection("edges", edge=True)

    nodes_col = db.collection("nodes")
    edges_col = db.collection("edges")

    # Insert nodes
    nodes_col.insert(
        {"_key": "A", "id": "A", "name": "A", "category": ["biolink:NamedThing"]}
    )
    nodes_col.insert(
        {"_key": "B", "id": "B", "name": "B", "category": ["biolink:NamedThing"]}
    )
    nodes_col.insert(
        {"_key": "C", "id": "C", "name": "C", "category": ["biolink:NamedThing"]}
    )

    # Insert edges
    edges_col.insert(
        {
            "_from": "nodes/A",
            "_to": "nodes/B",
            "subject": "A",
            "object": "B",
            "predicate": "biolink:related_to",
            "relation": "biolink:related_to",
        }
    )
    edges_col.insert(
        {
            "_from": "nodes/A",
            "_to": "nodes/C",
            "subject": "A",
            "object": "C",
            "predicate": "biolink:related_to",
            "relation": "biolink:related_to",
        }
    )

    t = Transformer()
    s = ArangoSource(t)

    g = s.parse(
        uri=DEFAULT_ARANGO_URL,
        database=DEFAULT_ARANGO_DATABASE,
        username=DEFAULT_ARANGO_USERNAME,
        password=DEFAULT_ARANGO_PASSWORD,
    )

    nodes, edges = load_graph_dictionary(g)

    assert len(nodes.keys()) == 3
    assert len(edges.keys()) == 2

    n1 = nodes["A"]
    assert n1["id"] == "A"
    assert n1["name"] == "A"
    assert "category" in n1 and "biolink:NamedThing" in n1["category"]

    e1 = edges[("A", "C")][0]
    assert e1["subject"] == "A"
    assert e1["object"] == "C"
    assert e1["predicate"] == "biolink:related_to"
    assert e1["relation"] == "biolink:related_to"


def _parse(**kwargs):
    t = Transformer()
    s = ArangoSource(t)
    g = s.parse(
        uri=DEFAULT_ARANGO_URL,
        database=DEFAULT_ARANGO_DATABASE,
        username=DEFAULT_ARANGO_USERNAME,
        password=DEFAULT_ARANGO_PASSWORD,
        node_collections=["CL", "UBERON"],
        edge_collections=["CL-CL", "CL-UBERON"],
        **kwargs,
    )
    return load_graph_dictionary(g)


@pytest.mark.skipif(
    not check_arango_container(),
    reason=f"Container {ARANGO_CONTAINER_NAME} is not running",
)
def test_read_arango_stored_id(ontology_collections):
    """
    By default a stored 'id' is the node id, and edge endpoints follow it,
    as for a database written by ArangoSink.
    """
    nodes, edges = _parse()

    assert set(nodes) == {"CL:9900001", "CL:1000300", "UBERON:0001992"}
    assert ("CL:9900001", "UBERON:0001992") in edges


@pytest.mark.skipif(
    not check_arango_container(),
    reason=f"Container {ARANGO_CONTAINER_NAME} is not running",
)
def test_read_arango_use_arango_id(ontology_collections):
    """
    With use_arango_id, node ids and edge endpoints come from the document
    handles, and a stored 'id' is kept under document_id_property.
    """
    nodes, edges = _parse(use_arango_id=True, document_id_property="oboInOwl_id")

    assert set(nodes) == {
        "CL:0020036",
        "CL:0020041",
        "CL:1000300",
        "UBERON:0001992",
    }
    assert nodes["CL:0020036"]["oboInOwl_id"] == "CL:9900001"
    assert nodes["CL:0020041"]["oboInOwl_id"] == "CL:9900001"
    assert nodes["CL:0020041"]["label"] == "oRGC4"
    assert nodes["CL:1000300"]["oboInOwl_id"] == "CL:1000300"
    assert "oboInOwl_id" not in nodes["UBERON:0001992"]
    assert "biolink:NamedThing" in nodes["CL:0020041"]["category"]

    edge = edges[("CL:0020041", "UBERON:0001992")][0]
    assert edge["subject"] == "CL:0020041"
    assert edge["object"] == "UBERON:0001992"
    assert edge["predicate"] == "biolink:related_to"
    assert "_id" not in edge
    assert "_from" not in edge and "_to" not in edge


@pytest.mark.skipif(
    not check_arango_container(),
    reason=f"Container {ARANGO_CONTAINER_NAME} is not running",
)
def test_read_arango_edge_key(ontology_collections):
    """
    Edges are keyed by their document handle, so parallel edges in one
    collection have distinct keys.
    """
    t = Transformer()
    s = ArangoSource(t)
    g = s.parse(
        uri=DEFAULT_ARANGO_URL,
        database=DEFAULT_ARANGO_DATABASE,
        username=DEFAULT_ARANGO_USERNAME,
        password=DEFAULT_ARANGO_PASSWORD,
        node_collections=["CL"],
        edge_collections=["CL-CL"],
        use_arango_id=True,
    )
    keys = [rec[2] for rec in g if len(rec) == 4]

    assert len(keys) == 2
    assert all(k.startswith("CL-CL/") for k in keys)
    assert len(set(keys)) == 2
    assert "key" not in s.edge_properties


@pytest.mark.skipif(
    not check_arango_container(),
    reason=f"Container {ARANGO_CONTAINER_NAME} is not running",
)
@pytest.mark.parametrize("use_arango_id", [False, True])
def test_arango_to_tsv_keeps_parallel_edges(ontology_collections, use_arango_id):
    """
    An export through the in-memory graph writes every ArangoDB edge,
    including edges between the same nodes in the same collection.
    """
    output = os.path.join(TARGET_DIR, f"arango_parallel_edges_{use_arango_id}")
    arango_download(
        DEFAULT_ARANGO_URL,
        DEFAULT_ARANGO_DATABASE,
        DEFAULT_ARANGO_USERNAME,
        DEFAULT_ARANGO_PASSWORD,
        output,
        "tsv",
        None,
        False,
        node_collections=["CL", "UBERON"],
        edge_collections=["CL-CL", "CL-UBERON"],
        use_arango_id=use_arango_id,
        document_id_property="oboInOwl_id",
    )
    nodes = _read_tsv(f"{output}_nodes.tsv")
    edges = _read_tsv(f"{output}_edges.tsv")

    synapses = [e for e in edges if e["Label"].startswith("SYNAPSED_")]
    labels = sorted(e["Label"] for e in synapses)
    assert labels == ["SYNAPSED_BY", "SYNAPSED_TO"]
    assert len(edges) == 3
    assert "key" not in edges[0]
    if use_arango_id:
        assert len(nodes) == 4
        assert {"CL:0020036", "CL:0020041"} <= {n["id"] for n in nodes}
        cl_cl = {(e["subject"], e["object"]) for e in synapses}
        assert cl_cl == {("CL:0020036", "CL:0020041")}
    else:
        # The stored ids merge oRGC1 and oRGC4, as before.
        assert len(nodes) == 3
