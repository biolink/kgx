import os

from kgx.source.sssom_source import SssomSource
from kgx.transformer import Transformer
from tests import RESOURCE_DIR
from tests.unit import load_graph_dictionary


def test_load1():
    """
    Read a SSSOM formatted file.
    """
    t = Transformer()
    source = SssomSource(t)

    g = source.parse(
        filename=os.path.join(RESOURCE_DIR, "sssom_example1.tsv"), format="sssom"
    )
    nodes, edges = load_graph_dictionary(g)
    assert len(nodes.keys()) == 18
    assert len(edges.keys()) == 9

    assert nodes["MP:0012051"]["id"] == "MP:0012051"
    assert nodes["HP:0001257"]["id"] == "HP:0001257"

    e = edges["MP:0012051", "HP:0001257"][0]
    assert e["subject"] == "MP:0012051"
    assert e["object"] == "HP:0001257"
    assert e["predicate"] == "biolink:same_as"
    assert e["confidence"] == "1.0"


def test_load2():
    """
    Read a SSSOM formatted file, with more metadata on mappings.
    """
    t = Transformer()
    source = SssomSource(t)

    g = source.parse(
        filename=os.path.join(RESOURCE_DIR, "sssom_example2.tsv"), format="sssom"
    )
    nodes, edges = load_graph_dictionary(g)
    assert len(nodes.keys()) == 18
    assert len(edges.keys()) == 9

    n1 = nodes["MP:0002152"]
    assert n1["id"] == "MP:0002152"

    n2 = nodes["HP:0012443"]
    assert n2["id"] == "HP:0012443"

    e = edges["MP:0002152", "HP:0012443"][0]
    assert e["subject"] == "MP:0002152"
    assert e["subject_label"] == "abnormal brain morphology"
    assert e["object"] == "HP:0012443"
    assert e["object_label"] == "Abnormality of brain morphology"
    assert e["predicate"] == "biolink:exact_match"
    assert e["match_type"] == "SSSOMC:Lexical"
    assert e["reviewer_id"] == "orcid:0000-0000-0000-0000"


def test_load3():
    """
    Read a SSSOM formatted file that has metadata provided in headers.
    """
    t = Transformer()
    source = SssomSource(t)

    g = source.parse(
        filename=os.path.join(RESOURCE_DIR, "sssom_example3.tsv"), format="sssom"
    )
    nodes, edges = load_graph_dictionary(g)
    assert len(nodes) == 20
    assert len(edges) == 10

    e = edges["MA:0000168", "UBERON:0000955"][0]
    assert (
        "mapping_provider" in e
        and e["mapping_provider"] == "https://www.mousephenotype.org"
    )
    assert (
        "mapping_set_group" in e and e["mapping_set_group"] == "impc_mouse_morphology"
    )
    assert "mapping_set_id" in e and e["mapping_set_id"] == "ma_uberon_impc_pat"
    assert (
        "mapping_set_title" in e
        and e["mapping_set_title"]
        == "The IMPC Mouse Morphology Mappings: Gross Pathology & Tissue Collection Test (Anatomy)"
    )
    assert (
        "creator_id" in e and e["creator_id"] == "https://orcid.org/0000-0000-0000-0000"
    )
    assert (
        "license" in e
        and e["license"] == "https://creativecommons.org/publicdomain/zero/1.0/"
    )
    assert "curie_map" not in e
