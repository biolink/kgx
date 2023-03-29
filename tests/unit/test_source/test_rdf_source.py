import os
from pprint import pprint

import pytest

from kgx.source import RdfSource
from kgx.transformer import Transformer
from tests import RESOURCE_DIR
from tests.unit import load_graph_dictionary


def test_read_nt1():
    """
    Read from an RDF N-Triple file using RdfSource.
    """
    t = Transformer()
    s = RdfSource(t)

    g = s.parse(os.path.join(RESOURCE_DIR, "rdf", "test1.nt"))
    nodes, edges = load_graph_dictionary(g)
    assert len(nodes) == 2
    assert len(edges) == 1

    n1 = nodes["ENSEMBL:ENSG0000000000001"]
    assert n1["type"] == ["SO:0000704"]
    assert len(n1["category"]) == 4
    assert "biolink:Gene" in n1["category"]
    assert "biolink:GenomicEntity" in n1["category"]
    assert "biolink:NamedThing" in n1["category"]
    assert n1["name"] == "Test Gene 123"
    assert n1["description"] == "This is a Test Gene 123"
    assert "Test Dataset" in n1["provided_by"]

    n2 = nodes["ENSEMBL:ENSG0000000000002"]
    assert n2["type"] == ["SO:0000704"]
    assert len(n2["category"]) == 4
    assert "biolink:Gene" in n2["category"]
    assert "biolink:GenomicEntity" in n2["category"]
    assert "biolink:NamedThing" in n1["category"]
    assert n2["name"] == "Test Gene 456"
    assert n2["description"] == "This is a Test Gene 456"
    assert "Test Dataset" in n2["provided_by"]

    e = list(edges.values())[0][0]
    assert e["subject"] == "ENSEMBL:ENSG0000000000001"
    assert e["object"] == "ENSEMBL:ENSG0000000000002"
    assert e["predicate"] == "biolink:interacts_with"
    assert e["relation"] == "biolink:interacts_with"


def test_read_nt2():
    """
    Read from an RDF N-Triple file using RdfSource.
    This test also supplies the knowledge_source parameter.
    """
    t = Transformer()
    s = RdfSource(t)

    g = s.parse(
        os.path.join(RESOURCE_DIR, "rdf", "test1.nt"),
        provided_by="Test Dataset",
        knowledge_source="Test Dataset",
    )
    nodes, edges = load_graph_dictionary(g)
    assert len(nodes) == 2
    assert len(edges) == 1

    n1 = nodes["ENSEMBL:ENSG0000000000001"]
    assert n1["type"] == ["SO:0000704"]
    assert len(n1["category"]) == 4
    assert "biolink:Gene" in n1["category"]
    assert "biolink:GenomicEntity" in n1["category"]
    assert "biolink:NamedThing" in n1["category"]
    assert n1["name"] == "Test Gene 123"
    assert n1["description"] == "This is a Test Gene 123"
    assert "Test Dataset" in n1["provided_by"]

    n2 = nodes["ENSEMBL:ENSG0000000000002"]
    assert n2["type"] == ["SO:0000704"]
    assert len(n2["category"]) == 4
    assert "biolink:Gene" in n2["category"]
    assert "biolink:GenomicEntity" in n2["category"]
    assert "biolink:NamedThing" in n1["category"]
    assert n2["name"] == "Test Gene 456"
    assert n2["description"] == "This is a Test Gene 456"
    assert "Test Dataset" in n2["provided_by"]

    e = list(edges.values())[0][0]
    assert e["subject"] == "ENSEMBL:ENSG0000000000001"
    assert e["object"] == "ENSEMBL:ENSG0000000000002"
    assert e["predicate"] == "biolink:interacts_with"
    assert e["relation"] == "biolink:interacts_with"
    assert "Test Dataset" in e["knowledge_source"]


def test_read_nt3():
    """
    Read from an RDF N-Triple file using RdfSource, with user defined
    node property predicates.
    """
    node_property_predicates = {
        f"https://www.example.org/UNKNOWN/{x}"
        for x in ["fusion", "homology", "combined_score", "cooccurence"]
    }

    t = Transformer()
    source = RdfSource(t)

    source.set_node_property_predicates(node_property_predicates)
    g = source.parse(
        filename=os.path.join(RESOURCE_DIR, "rdf", "test2.nt"), format="nt"
    )
    nodes, edges = load_graph_dictionary(g)
    assert len(nodes) == 4
    assert len(edges) == 3

    n1 = nodes["ENSEMBL:ENSG0000000000001"]
    assert n1["type"] == ["SO:0000704"]
    assert len(n1["category"]) == 4
    assert "biolink:Gene" in n1["category"]
    assert "biolink:GenomicEntity" in n1["category"]
    assert "biolink:NamedThing" in n1["category"]
    assert n1["name"] == "Test Gene 123"
    assert n1["description"] == "This is a Test Gene 123"
    assert "Test Dataset" in n1["provided_by"]

    n2 = nodes["ENSEMBL:ENSG0000000000002"]
    assert n2["type"] == ["SO:0000704"]
    assert len(n2["category"]) == 4
    assert "biolink:Gene" in n2["category"]
    assert "biolink:GenomicEntity" in n2["category"]
    assert "biolink:NamedThing" in n1["category"]
    assert n2["name"] == "Test Gene 456"
    assert n2["description"] == "This is a Test Gene 456"
    assert "Test Dataset" in n2["provided_by"]

    e1 = edges["ENSEMBL:ENSP0000000000001", "ENSEMBL:ENSP0000000000002"][0]
    assert e1["subject"] == "ENSEMBL:ENSP0000000000001"
    assert e1["object"] == "ENSEMBL:ENSP0000000000002"
    assert e1["predicate"] == "biolink:interacts_with"
    assert e1["relation"] == "biolink:interacts_with"
    assert e1["type"] == ["biolink:Association"]
    assert e1["id"] == "urn:uuid:fcf76807-f909-4ccb-b40a-3b79b49aa518"
    assert e1["fusion"] == "0"
    assert e1["homology"] == "0.0"
    assert e1["combined_score"] == "490.0"
    assert e1["cooccurence"] == "332"


def test_read_nt4():
    """
    Read from an RDF N-Triple file using RdfSource, with user defined
    node property predicates.
    """
    node_property_predicates = {
        f"https://www.example.org/UNKNOWN/{x}"
        for x in ["fusion", "homology", "combined_score", "cooccurence"]
    }

    t = Transformer()
    source = RdfSource(t)

    source.set_node_property_predicates(node_property_predicates)
    g = source.parse(
        filename=os.path.join(RESOURCE_DIR, "rdf", "test3.nt"), format="nt"
    )
    nodes, edges = load_graph_dictionary(g)
    assert len(nodes.keys()) == 7
    assert len(edges.keys()) == 6

    n1 = nodes["ENSEMBL:ENSG0000000000001"]
    assert n1["type"] == ["SO:0000704"]
    assert len(n1["category"]) == 4
    assert "biolink:Gene" in n1["category"]
    assert "biolink:GenomicEntity" in n1["category"]
    assert "biolink:NamedThing" in n1["category"]
    assert n1["name"] == "Test Gene 123"
    assert n1["description"] == "This is a Test Gene 123"
    assert "Test Dataset" in n1["provided_by"]

    n2 = nodes["ENSEMBL:ENSG0000000000002"]
    assert n2["type"] == ["SO:0000704"]
    assert len(n2["category"]) == 4
    assert "biolink:Gene" in n2["category"]
    assert "biolink:GenomicEntity" in n2["category"]
    assert "biolink:NamedThing" in n1["category"]
    assert n2["name"] == "Test Gene 456"
    assert n2["description"] == "This is a Test Gene 456"
    assert "Test Dataset" in n2["provided_by"]

    e1 = edges["ENSEMBL:ENSP0000000000001", "ENSEMBL:ENSP0000000000002"][0]
    assert e1["subject"] == "ENSEMBL:ENSP0000000000001"
    assert e1["object"] == "ENSEMBL:ENSP0000000000002"
    assert e1["predicate"] == "biolink:interacts_with"
    assert e1["relation"] == "biolink:interacts_with"
    assert e1["type"] == ["biolink:Association"]
    assert e1["id"] == "urn:uuid:fcf76807-f909-4ccb-b40a-3b79b49aa518"
    assert e1["fusion"] == "0"
    assert e1["homology"] == "0.0"
    assert e1["combined_score"] == "490.0"
    assert e1["cooccurence"] == "332"

    e2 = edges["ENSEMBL:ENSP0000000000001", "UniProtKB:X0000001"][0]
    assert e2["subject"] == "ENSEMBL:ENSP0000000000001"
    assert e2["object"] == "UniProtKB:X0000001"
    assert e2["predicate"] == "biolink:same_as"
    assert e2["relation"] == "owl:equivalentClass"

    e3 = edges["ENSEMBL:ENSP0000000000001", "MONDO:0000001"][0]
    assert e3["subject"] == "ENSEMBL:ENSP0000000000001"
    assert e3["object"] == "MONDO:0000001"
    assert e3["predicate"] == "biolink:treats"
    assert e3["relation"] == "RO:0002606"


def test_read_nt5():
    """
    Parse an OBAN styled NT, with user defined prefix_map and node_property_predicates.
    """
    prefix_map = {
        "HGNC": "https://www.genenames.org/data/gene-symbol-report/#!/hgnc_id/",
        "OMIM": "http://omim.org/entry/",
    }
    node_property_predicates = {
        "http://purl.obolibrary.org/obo/RO_0002558",
        "http://purl.org/dc/elements/1.1/source",
        "https://monarchinitiative.org/frequencyOfPhenotype",
    }
    filename = os.path.join(RESOURCE_DIR, "rdf", "oban-test.nt")

    t = Transformer()
    source = RdfSource(t)

    source.set_prefix_map(prefix_map)
    source.set_node_property_predicates(node_property_predicates)
    g = source.parse(filename=filename, format="nt")
    nodes, edges = load_graph_dictionary(g)

    assert len(nodes.keys()) == 14
    assert len(edges.keys()) == 7

    n1 = nodes["HP:0000505"]
    assert len(n1["category"]) == 1
    assert "biolink:NamedThing" in n1["category"]

    e1 = edges["OMIM:166400", "HP:0000006"][0]
    assert e1["subject"] == "OMIM:166400"
    assert e1["object"] == "HP:0000006"
    assert e1["relation"] == "RO:0000091"
    assert e1["type"] == ["OBAN:association"]
    assert e1["has_evidence"] == ["ECO:0000501"]

    e2 = edges["ORPHA:93262", "HP:0000505"][0]
    assert e2["subject"] == "ORPHA:93262"
    assert e2["object"] == "HP:0000505"
    assert e2["relation"] == "RO:0002200"
    assert e2["type"] == ["OBAN:association"]
    assert e2["frequencyOfPhenotype"] == "HP:0040283"


def test_read_nt6():
    prefix_map = {
        "HGNC": "https://www.genenames.org/data/gene-symbol-report/#!/hgnc_id/",
        "OMIM": "http://omim.org/entry/",
    }
    node_property_predicates = {
        "http://purl.obolibrary.org/obo/RO_0002558",
        "http://purl.org/dc/elements/1.1/source",
        "https://monarchinitiative.org/frequencyOfPhenotype",
    }
    predicate_mapping = {
        "https://monarchinitiative.org/frequencyOfPhenotype": "frequency_of_phenotype"
    }
    filename = os.path.join(RESOURCE_DIR, "rdf", "oban-test.nt")

    t = Transformer()
    source = RdfSource(t)

    source.set_prefix_map(prefix_map)
    source.set_node_property_predicates(node_property_predicates)
    source.set_predicate_mapping(predicate_mapping)

    g = source.parse(filename=filename, format="nt")
    nodes, edges = load_graph_dictionary(g)

    assert len(nodes.keys()) == 14
    assert len(edges.keys()) == 7

    n1 = nodes["HP:0000505"]
    assert len(n1["category"]) == 1
    assert "biolink:NamedThing" in n1["category"]

    e1 = edges["OMIM:166400", "HP:0000006"][0]
    assert e1["subject"] == "OMIM:166400"
    assert e1["object"] == "HP:0000006"
    assert e1["relation"] == "RO:0000091"
    assert e1["type"] == ["OBAN:association"]
    assert e1["has_evidence"] == ["ECO:0000501"]

    e2 = edges["ORPHA:93262", "HP:0000505"][0]
    assert e2["subject"] == "ORPHA:93262"
    assert e2["object"] == "HP:0000505"
    assert e2["relation"] == "RO:0002200"
    assert e2["type"] == ["OBAN:association"]
    assert e2["frequency_of_phenotype"] == "HP:0040283"


@pytest.mark.parametrize(
    "query",
    [
        (
            {"id": "ABC:123", "category": "biolink:NamedThing", "prop1": [1, 2, 3]},
            {"category": ["biolink:NamedThing", "biolink:Gene"], "prop1": [4]},
            {"category": ["biolink:NamedThing", "biolink:Gene"]},
            {"prop1": [1, 2, 3, 4]},
        ),
        (
            {"id": "ABC:123", "category": ["biolink:NamedThing"], "prop1": 1},
            {"category": {"biolink:NamedThing", "biolink:Gene"}, "prop1": [2, 3]},
            {"category": ["biolink:NamedThing", "biolink:Gene"]},
            {"prop1": [1, 2, 3]},
        ),
        (
            {
                "id": "ABC:123",
                "category": ["biolink:NamedThing"],
                "provided_by": "test1",
            },
            {
                "id": "DEF:456",
                "category": ("biolink:NamedThing", "biolink:Gene"),
                "provided_by": "test2",
            },
            {"category": ["biolink:NamedThing", "biolink:Gene"]},
            {"provided_by": ["test1", "test2"]},
        ),
        (
            {
                "subject": "Orphanet:331206",
                "object": "HP:0004429",
                "relation": "RO:0002200",
                "predicate": "biolink:has_phenotype",
                "id": "bfada868a8309f2b7849",
                "type": "OBAN:association",
            },
            {
                "subject": "Orphanet:331206",
                "object": "HP:0004429",
                "relation": "RO:0002200",
                "predicate": "biolink:has_phenotype",
                "id": "bfada868a8309f2b7849",
                "type": "OBAN:association",
            },
            {},
            {},
        ),
        (
            {
                "subject": "Orphanet:331206",
                "object": "HP:0004429",
                "relation": "RO:0002200",
                "predicate": "biolink:has_phenotype",
                "id": "bfada868a8309f2b7849",
                "type": "OBAN:association",
                "knowledge_source": "Orphanet:331206",
            },
            {
                "subject": "Orphanet:331206",
                "object": "HP:0004429",
                "relation": "RO:0002200",
                "predicate": "biolink:has_phenotype",
                "id": "bfada868a8309f2b7849",
                "type": "OBAN:association",
                "knowledge_source": "Orphanet:331206",
            },
            {},
            {"knowledge_source": ["Orphanet:331206"]},
        ),
    ],
)
def test_prepare_data_dict(query):
    """
    Test for internal _prepare_data_dict method in RdfSource.
    """
    t = Transformer()
    source = RdfSource(t)
    new_data = source._prepare_data_dict(query[0], query[1])
    for k, v in query[2].items():
        assert new_data[k] == v
    for k, v in query[3].items():
        assert new_data[k] == v
