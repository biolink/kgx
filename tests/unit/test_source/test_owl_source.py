import os

from kgx.source import OwlSource
from kgx.transformer import Transformer
from tests import RESOURCE_DIR
from pprint import pprint


def test_read_owl1():
    """
    Read an OWL ontology using OwlSource.
    """
    t = Transformer()
    s = OwlSource(t)
    g = s.parse(
        os.path.join(RESOURCE_DIR, "goslim_generic.owl"),
        provided_by="GO slim generic",
        knowledge_source="GO slim generic"
    )
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    n1 = nodes["GO:0008150"]
    assert n1["name"] == "biological_process"
    assert "has_exact_synonym" in n1
    assert "description" in n1
    assert "comment" in n1
    assert "has_alternative_id" in n1
    assert "has_exact_synonym" in n1
    assert "physiological process" in n1["has_exact_synonym"]

    n2 = nodes["GO:0003674"]
    n2["name"] = "molecular_function"
    assert "has_exact_synonym" in n2
    assert "description" in n2
    assert "comment" in n2
    assert "has_alternative_id" in n2

    n3 = nodes["GO:0005575"]
    n3["name"] = "cellular_component"
    assert "has_exact_synonym" in n3
    assert "description" in n3
    assert "comment" in n3
    assert "has_alternative_id" in n3
    assert "GO:0008372" in n3["has_alternative_id"]

    e1 = edges["GO:0008289", "GO:0003674"]
    assert e1["subject"] == "GO:0008289"
    assert e1["predicate"] == "biolink:subclass_of"
    assert e1["object"] == "GO:0003674"
    assert e1["relation"] == "rdfs:subClassOf"


def test_read_owl2():
    """
    Read an OWL ontology using OwlSource.
    This test also supplies the knowledge_source parameter.
    """
    t = Transformer()
    s = OwlSource(t)

    g = s.parse(
        os.path.join(RESOURCE_DIR, "goslim_generic.owl"),
        provided_by="GO slim generic",
        knowledge_source="GO slim generic",
    )
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    n1 = nodes["GO:0008150"]
    assert n1["name"] == "biological_process"
    assert "has_exact_synonym" in n1
    assert "description" in n1
    assert "comment" in n1
    assert "has_alternative_id" in n1
    assert "GO slim generic" in n1["provided_by"]

    n2 = nodes["GO:0003674"]
    n2["name"] = "molecular_function"
    assert "has_exact_synonym" in n2
    assert "description" in n2
    assert "comment" in n2
    assert "has_alternative_id" in n2
    assert "GO slim generic" in n2["provided_by"]

    n3 = nodes["GO:0005575"]
    n3["name"] = "cellular_component"
    assert "has_exact_synonym" in n3
    assert "description" in n3
    assert "comment" in n3
    assert "has_alternative_id" in n3
    assert "GO slim generic" in n3["provided_by"]

    e1 = edges["GO:0008289", "GO:0003674"]
    assert e1["subject"] == "GO:0008289"
    assert e1["predicate"] == "biolink:subclass_of"
    assert e1["object"] == "GO:0003674"
    assert e1["relation"] == "rdfs:subClassOf"
    assert "GO slim generic" in e1["knowledge_source"]


def test_read_owl3():
    """
    Read an OWL ontology, with user defined
    node property predicates and predicate mappings.
    """
    node_property_predicates = {"http://www.geneontology.org/formats/oboInOwl#inSubset"}
    predicate_mappings = {
        "http://www.geneontology.org/formats/oboInOwl#inSubset": "subsets",
        "http://www.geneontology.org/formats/oboInOwl#hasOBONamespace": "namespace",
        "http://www.geneontology.org/formats/oboInOwl#hasAlternativeId": "xref",
    }

    t = Transformer()
    source = OwlSource(t)

    source.set_predicate_mapping(predicate_mappings)
    source.set_node_property_predicates(node_property_predicates)
    g = source.parse(filename=os.path.join(RESOURCE_DIR, "goslim_generic.owl"))
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                key = (rec[0], rec[1])
                if key in edges:
                    edges[key].append(rec[-1])
                else:
                    edges[key] = [rec[-1]]
            else:
                nodes[rec[0]] = rec[-1]

    n1 = nodes["GO:0008150"]
    pprint(n1)
    assert n1["name"] == "biological_process"
    assert "subsets" in n1 and "GOP:goslim_generic" in n1["subsets"]
    assert "has_exact_synonym" in n1
    assert "description" in n1
    assert "comment" in n1
    assert "xref" in n1 and "GO:0044699" in n1["xref"]

    n2 = nodes["GO:0003674"]
    n2["name"] = "molecular_function"
    assert "subsets" in n2 and "GOP:goslim_generic" in n2["subsets"]
    assert "has_exact_synonym" in n2
    assert "description" in n2
    assert "comment" in n2
    assert "xref" in n2 and "GO:0005554" in n2["xref"]

    n3 = nodes["GO:0005575"]
    n3["name"] = "cellular_component"
    assert "subsets" in n3 and "GOP:goslim_generic" in n3["subsets"]
    assert "has_exact_synonym" in n3
    assert "description" in n3
    assert "comment" in n3
    assert "xref" in n3 and "GO:0008372" in n3["xref"]

    e1 = edges["GO:0008289", "GO:0003674"][0]
    assert e1["subject"] == "GO:0008289"
    assert e1["predicate"] == "biolink:subclass_of"
    assert e1["object"] == "GO:0003674"
    assert e1["relation"] == "rdfs:subClassOf"


def test_read_owl4():
    """
    Read an OWL and ensure that logical axioms are annotated with Owlstar vocabulary.
    """
    t = Transformer()
    source = OwlSource(t)
    g = source.parse(
        filename=os.path.join(RESOURCE_DIR, "goslim_generic.owl"), format="owl"
    )
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                key = (rec[0], rec[1])
                if key in edges:
                    edges[key].append(rec[-1])
                else:
                    edges[key] = [rec[-1]]
            else:
                nodes[rec[0]] = rec[-1]

    e1 = edges["GO:0031012", "GO:0005576"][0]
    assert e1["predicate"] == "biolink:part_of"
    assert e1["relation"] == "BFO:0000050"
    assert (
        "logical_interpretation" in e1
        and e1["logical_interpretation"] == "owlstar:AllSomeInterpretation"
    )
    e2 = edges["GO:0030705", "GO:0005622"][0]
    assert e2["predicate"] == "biolink:occurs_in"
    assert e2["relation"] == "BFO:0000066"
    assert (
        "logical_interpretation" in e2
        and e2["logical_interpretation"] == "owlstar:AllSomeInterpretation"
    )
