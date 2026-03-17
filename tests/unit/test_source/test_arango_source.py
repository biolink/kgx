import pytest

from kgx.source import ArangoSource
from kgx.transformer import Transformer
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
