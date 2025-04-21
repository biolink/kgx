import os
import json
import tempfile
import gzip

from kgx.source import TrapiSource
from kgx.transformer import Transformer
from tests import RESOURCE_DIR, TARGET_DIR


def test_read_trapi_json1():
    """
    Read from a JSON using TrapiSource.
    """
    t = Transformer()
    s = TrapiSource(t)

    g = s.parse(os.path.join(RESOURCE_DIR, "rsa_sample.json"))
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    assert len(nodes.keys()) == 4
    assert len(edges.keys()) == 3

    n = nodes["HGNC:11603"]
    assert n["id"] == "HGNC:11603"
    assert n["name"] == "TBX4"
    assert n["category"] == ["biolink:Gene"]

    e = edges["HGNC:11603", "MONDO:0005002"]
    assert e["subject"] == "HGNC:11603"
    assert e["object"] == "MONDO:0005002"
    assert e["predicate"] == "biolink:related_to"


def test_read_trapi_json2():
    """
    Read from a TRAPI JSON using TrapiSource.
    This test also supplies the knowledge_source parameter.
    """
    t = Transformer()
    s = TrapiSource(t)

    g = s.parse(
        os.path.join(RESOURCE_DIR, "rsa_sample.json"),
        provided_by="Test TRAPI JSON",
        knowledge_source="Test TRAPI JSON",
    )
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    assert len(nodes.keys()) == 4
    assert len(edges.keys()) == 3

    n = nodes["HGNC:11603"]
    assert n["id"] == "HGNC:11603"
    assert n["name"] == "TBX4"
    assert n["category"] == ["biolink:Gene"]
    assert "Test TRAPI JSON" in n["provided_by"]

    e = edges["HGNC:11603", "MONDO:0005002"]
    assert e["subject"] == "HGNC:11603"
    assert e["object"] == "MONDO:0005002"
    assert e["predicate"] == "biolink:related_to"
    assert "Test TRAPI JSON" in e["knowledge_source"]


def test_read_trapi_json_with_categories():
    """
    Read from a TRAPI JSON with 'categories' field.
    """
    t = Transformer()
    s = TrapiSource(t)
    
    # Create a temporary TRAPI JSON file with 'categories' instead of 'type'
    temp_trapi = {
        "knowledge_graph": {
            "nodes": {
                "HGNC:12345": {
                    "name": "Test Gene",
                    "categories": ["biolink:Gene"]
                },
                "MONDO:6789": {
                    "name": "Test Disease",
                    "categories": ["biolink:Disease"]
                }
            },
            "edges": {
                "e1": {
                    "subject": "HGNC:12345",
                    "predicate": "biolink:gene_associated_with_condition",
                    "object": "MONDO:6789",
                    "sources": [
                        {
                            "resource_id": "infores:test-source",
                            "resource_role": "primary_knowledge_source"
                        }
                    ]
                }
            }
        }
    }
    
    temp_file = os.path.join(TARGET_DIR, "test_trapi_categories.json")
    with open(temp_file, 'w') as f:
        json.dump(temp_trapi, f)
    
    # Parse the temporary file
    g = s.parse(temp_file)
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]
    
    # Check that categories are correctly mapped to category
    assert len(nodes.keys()) == 2
    assert nodes["HGNC:12345"]["category"] == ["biolink:Gene"]
    assert nodes["MONDO:6789"]["category"] == ["biolink:Disease"]
    
    # Check edge
    assert len(edges.keys()) == 1
    e = edges["HGNC:12345", "MONDO:6789"]
    assert e["predicate"] == "biolink:gene_associated_with_condition"
    
    # Check source mapping
    assert "primary_knowledge_source" in e
    assert e["primary_knowledge_source"] == "infores:test-source"


def test_read_trapi_json_with_attributes():
    """
    Read from a TRAPI JSON with attributes.
    """
    t = Transformer()
    s = TrapiSource(t)
    
    # Create a temporary TRAPI JSON file with attributes
    temp_trapi = {
        "knowledge_graph": {
            "nodes": {
                "HGNC:12345": {
                    "name": "Test Gene",
                    "categories": ["biolink:Gene"],
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:description",
                            "value": "This is a test gene"
                        },
                        {
                            "attribute_type_id": "biolink:xref",
                            "value": ["XREF:123", "XREF:456"]
                        }
                    ]
                }
            },
            "edges": {
                "e1": {
                    "subject": "HGNC:12345",
                    "predicate": "biolink:gene_associated_with_condition",
                    "object": "MONDO:6789",
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:provided_by",
                            "value": ["source1", "source2"]
                        },
                        {
                            "attribute_type_id": "biolink:p_value",
                            "value": 0.001
                        }
                    ]
                }
            }
        }
    }
    
    temp_file = os.path.join(TARGET_DIR, "test_trapi_attributes.json")
    with open(temp_file, 'w') as f:
        json.dump(temp_trapi, f)
    
    # Parse the temporary file
    g = s.parse(temp_file)
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]
    
    # Check that attributes are correctly mapped
    assert "description" in nodes["HGNC:12345"]
    assert nodes["HGNC:12345"]["description"] == "This is a test gene"
    assert "xref" in nodes["HGNC:12345"]
    assert nodes["HGNC:12345"]["xref"] == ["XREF:123", "XREF:456"]
    
    # Check edge attributes
    e = edges["HGNC:12345", "MONDO:6789"]
    assert "provided_by" in e
    assert e["provided_by"] == ["source1", "source2"]
    assert "p_value" in e
    assert e["p_value"] == 0.001


def test_read_trapi_json_with_qualifiers():
    """
    Read from a TRAPI JSON with edge qualifiers.
    """
    t = Transformer()
    s = TrapiSource(t)
    
    # Create a temporary TRAPI JSON file with qualifiers
    temp_trapi = {
        "knowledge_graph": {
            "nodes": {
                "HGNC:12345": {
                    "name": "Test Gene",
                    "categories": ["biolink:Gene"]
                },
                "MONDO:6789": {
                    "name": "Test Disease",
                    "categories": ["biolink:Disease"]
                }
            },
            "edges": {
                "e1": {
                    "subject": "HGNC:12345",
                    "predicate": "biolink:affects",
                    "object": "MONDO:6789",
                    "qualifiers": [
                        {
                            "qualifier_type_id": "biolink:object_aspect_qualifier",
                            "qualifier_value": "activity"
                        },
                        {
                            "qualifier_type_id": "biolink:object_direction_qualifier",
                            "qualifier_value": "decreased"
                        }
                    ]
                }
            }
        }
    }
    
    temp_file = os.path.join(TARGET_DIR, "test_trapi_qualifiers.json")
    with open(temp_file, 'w') as f:
        json.dump(temp_trapi, f)
    
    # Parse the temporary file
    g = s.parse(temp_file)
    edges = {}
    for rec in g:
        if rec and len(rec) == 4:
            edges[(rec[0], rec[1])] = rec[3]
    
    # Check edge qualifiers
    e = edges["HGNC:12345", "MONDO:6789"]
    assert "qualifiers" in e
    
    # First qualifier
    assert e["qualifiers"][0]["qualifier_type_id"] == "biolink:object_aspect_qualifier"
    assert e["qualifiers"][0]["qualifier_value"] == "activity"
    
    # Second qualifier
    assert e["qualifiers"][1]["qualifier_type_id"] == "biolink:object_direction_qualifier"
    assert e["qualifiers"][1]["qualifier_value"] == "decreased"


def test_read_trapi_jsonl():
    """
    Read from a TRAPI JSONL file.
    """
    t = Transformer()
    s = TrapiSource(t)
    
    # Create a temporary TRAPI JSONL file
    temp_jsonl = [
        {"type": "knowledge_graph", "biolink_version": "2.4.8"},
        {"type": "node", "id": "HGNC:12345", "name": "Test Gene", "categories": ["biolink:Gene"]},
        {"type": "node", "id": "MONDO:6789", "name": "Test Disease", "categories": ["biolink:Disease"]},
        {
            "type": "edge", 
            "id": "e1", 
            "subject": "HGNC:12345", 
            "predicate": "biolink:gene_associated_with_condition", 
            "object": "MONDO:6789"
        }
    ]
    
    temp_file = os.path.join(TARGET_DIR, "test_trapi.jsonl")
    with open(temp_file, 'w') as f:
        for line in temp_jsonl:
            f.write(json.dumps(line) + "\n")
    
    # Parse the temporary file
    g = s.parse(temp_file, format="jsonl")
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]
    
    # Check nodes and edges
    assert len(nodes.keys()) == 2
    assert nodes["HGNC:12345"]["name"] == "Test Gene"
    assert nodes["MONDO:6789"]["name"] == "Test Disease"
    
    assert len(edges.keys()) == 1
    e = edges["HGNC:12345", "MONDO:6789"]
    assert e["predicate"] == "biolink:gene_associated_with_condition"
    
    # Check biolink version
    assert s.biolink_version == "2.4.8"


def test_read_compressed_trapi():
    """
    Read from a compressed TRAPI JSON file.
    """
    t = Transformer()
    s = TrapiSource(t)
    
    # Create a temporary compressed TRAPI JSON file
    temp_trapi = {
        "knowledge_graph": {
            "nodes": {
                "HGNC:12345": {
                    "name": "Test Gene",
                    "categories": ["biolink:Gene"]
                },
                "MONDO:6789": {
                    "name": "Test Disease",
                    "categories": ["biolink:Disease"]
                }
            },
            "edges": {
                "e1": {
                    "subject": "HGNC:12345",
                    "predicate": "biolink:gene_associated_with_condition",
                    "object": "MONDO:6789"
                }
            }
        }
    }
    
    temp_file = os.path.join(TARGET_DIR, "test_trapi_compressed.json.gz")
    with gzip.open(temp_file, 'wt') as f:
        json.dump(temp_trapi, f)
    
    # Parse the compressed file
    g = s.parse(temp_file, compression="gz")
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]
    
    # Check nodes and edges
    assert len(nodes.keys()) == 2
    assert len(edges.keys()) == 1
