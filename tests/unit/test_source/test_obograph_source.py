import os

import pytest

from kgx.cli import transform
from kgx.source import ObographSource, TsvSource
from kgx.transformer import Transformer
from tests import RESOURCE_DIR, TARGET_DIR


def test_read_obograph1():
    """
    Read from an Obograph JSON using ObographSource.
    """
    t = Transformer()
    s = ObographSource(t)
    g = s.parse(
        os.path.join(RESOURCE_DIR, "goslim_generic.json"),
        knowledge_source="GO slim generic",
    )
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1], rec[2])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    assert len(nodes) == 176
    assert len(edges) == 205

    n1 = nodes["GO:0003677"]
    assert n1["id"] == "GO:0003677"
    assert n1["name"] == "DNA binding"
    assert (
        n1["description"]
        == "Any molecular function by which a gene product interacts selectively and non-covalently with DNA (deoxyribonucleic acid)."
    )
    assert n1["category"] == ["biolink:MolecularActivity"]
    assert "structure-specific DNA binding" in n1["synonym"]

    assert "microtubule/chromatin interaction" in n1["synonym"]
    assert "plasmid binding" in n1["synonym"]

    # related and narrow synonym
    assert n1["related_synonym"] == ['structure-specific DNA binding','structure specific DNA binding','microtubule/chromatin interaction']
    assert n1["narrow_synonym"] == ['plasmid binding']

    n2 = nodes["GO:0005575"]
    assert n2["id"] == "GO:0005575"
    assert n2["name"] == "cellular_component"
    assert (
        n2["description"]
        == "A location, relative to cellular compartments and structures, occupied by a macromolecular machine when it carries out a molecular function. There are two ways in which the gene ontology describes locations of gene products: (1) relative to cellular structures (e.g., cytoplasmic side of plasma membrane) or compartments (e.g., mitochondrion), and (2) the stable macromolecular complexes of which they are parts (e.g., the ribosome)."
    )
    assert n2["category"] == ["biolink:CellularComponent"]
    assert n2["xref"] == ["NIF_Subcellular:sao1337158144"]
    assert "goslim_chembl" in n2["subsets"]
    assert "goslim_generic" in n2["subsets"]

    # just for exact synonym
    n3 = nodes["GO:0005975"]
    assert n3["exact_synonym"] == ['carbohydrate metabolism']

    # brad_synonym
    n5 = nodes["GO:0003924"]
    assert n5['broad_synonym'][0].startswith('hydrolase activity')


def test_read_jsonl2():
    """
    Read from an Obograph JSON using ObographSource.
    This test also supplies the provided_by parameter.
    """
    t = Transformer()
    s = ObographSource(t)
    g = s.parse(
        os.path.join(RESOURCE_DIR, "goslim_generic.json"),
        provided_by="GO slim generic",
        knowledge_source="GO slim generic",
    )
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1], rec[2])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    assert len(nodes) == 176
    assert len(edges) == 205

    n1 = nodes["GO:0003677"]
    assert n1["id"] == "GO:0003677"
    assert n1["name"] == "DNA binding"
    assert (
        n1["description"]
        == "Any molecular function by which a gene product interacts selectively and non-covalently with DNA (deoxyribonucleic acid)."
    )
    assert n1["category"] == ["biolink:MolecularActivity"]
    assert "structure-specific DNA binding" in n1["synonym"]
    assert "structure specific DNA binding" in n1["synonym"]
    assert "microtubule/chromatin interaction" in n1["synonym"]
    assert "plasmid binding" in n1["synonym"]
    assert "GO slim generic" in n1["provided_by"]

    n2 = nodes["GO:0005575"]
    assert n2["id"] == "GO:0005575"
    assert n2["name"] == "cellular_component"
    assert (
        n2["description"]
        == "A location, relative to cellular compartments and structures, occupied by a macromolecular machine when it carries out a molecular function. There are two ways in which the gene ontology describes locations of gene products: (1) relative to cellular structures (e.g., cytoplasmic side of plasma membrane) or compartments (e.g., mitochondrion), and (2) the stable macromolecular complexes of which they are parts (e.g., the ribosome)."
    )
    assert n2["category"] == ["biolink:CellularComponent"]
    assert n2["xref"] == ["NIF_Subcellular:sao1337158144"]
    assert "goslim_chembl" in n2["subsets"]
    assert "goslim_generic" in n2["subsets"]
    assert "GO slim generic" in n2["provided_by"]


def test_read_deprecated_term():
    """
    Read from an PATO JSON using ObographSource,
    to validate capture of "deprecate" status
    """
    t = Transformer()
    s = ObographSource(t)
    g = s.parse(
        os.path.join(RESOURCE_DIR, "pato.json"),
        knowledge_source="Phenotype and Trait Ontology",
    )
    nodes = {}
    for rec in g:
        if rec:
            if len(rec) != 4:
                nodes[rec[0]] = rec[1]

    n1 = nodes["PATO:0000000"]
    assert n1["id"] == "PATO:0000000"
    assert n1["name"] == "obsolete pato"
    assert n1["deprecated"] is True


def test_read_deprecated_term_phenio():
    """
    Read from a Phenio JSON using ObographSource,
    to validate capture of "deprecate" status
    """
    t = Transformer()
    s = ObographSource(t)
    g = s.parse(
        os.path.join(RESOURCE_DIR, "phenio.json"),
        knowledge_source="Phenomics Integrative Ontology",
    )
    nodes = {}
    for rec in g:
        if rec:
            if len(rec) != 4:
                nodes[rec[0]] = rec[1]

    n1 = nodes["GO:0051370"]
    assert n1["id"] == "GO:0051370"
    assert n1["name"] == "obsolete ZASP binding"
    assert n1["deprecated"] is True


@pytest.mark.parametrize(
    "query",
    [
        (
            {
                "id": "http://purl.obolibrary.org/obo/GO_0005615",
                "meta": {
                    "basicPropertyValues": [
                        {
                            "pred": "http://www.geneontology.org/formats/oboInOwl#hasOBONamespace",
                            "val": "cellular_component",
                        }
                    ]
                },
                "type": "CLASS",
                "lbl": "extracellular space",
            },
            "biolink:CellularComponent",
        ),
        (
            {
                "id": "http://purl.obolibrary.org/obo/GO_0008168",
                "meta": {
                    "definition": {
                        "val": "Catalysis of the transfer of a methyl group to an acceptor molecule."
                    },
                    "basicPropertyValues": [
                        {
                            "pred": "http://www.geneontology.org/formats/oboInOwl#hasAlternativeId",
                            "val": "GO:0004480",
                        },
                        {
                            "pred": "http://www.geneontology.org/formats/oboInOwl#hasOBONamespace",
                            "val": "molecular_function",
                        },
                    ],
                },
            },
            "biolink:MolecularActivity",
        ),
        (
            {
                "id": "http://purl.obolibrary.org/obo/GO_0065003",
                "meta": {
                    "definition": {
                        "val": "The aggregation, arrangement and bonding together of a set of macromolecules to form a protein-containing complex."
                    },
                    "basicPropertyValues": [
                        {
                            "pred": "http://www.geneontology.org/formats/oboInOwl#hasAlternativeId",
                            "val": "GO:0006461",
                        },
                        {
                            "pred": "http://www.geneontology.org/formats/oboInOwl#hasOBONamespace",
                            "val": "biological_process",
                        },
                    ],
                },
            },
            "biolink:BiologicalProcess",
        ),

    ],
)
def test_get_category(query):
    """
    Test to guess the appropriate category for a sample OBO Graph JSON.
    """
    node = query[0]

    t = Transformer()
    s = ObographSource(t)

    c = s.get_category(node["id"], node)
    assert c == query[1]


def test_error_detection():
    t = Transformer()
    s = ObographSource(t)
    g = s.parse(
        os.path.join(RESOURCE_DIR, "obo_error_detection.json"),
        knowledge_source="Sample OBO",
    )
    nodes = {}
    edges = {}
    for rec in g:
        if rec:
            if len(rec) == 4:
                edges[(rec[0], rec[1], rec[2])] = rec[3]
            else:
                nodes[rec[0]] = rec[1]

    assert len(t.get_errors()) > 0
    if len(t.get_errors("Error")) > 0:
        t.write_report(None, "Error")
    if len(t.get_errors("Warning")) > 0:
        t.write_report(None, "Warning")


def test_phenio_obojson_to_tsv():
    """
    Testing transitive propagation of node properties
    (mainly node 'deprecated' status)
    from a Phenio JSON to TSV file format
    """
    transform(
        inputs=[os.path.join(RESOURCE_DIR, "phenio.json")],
        input_format="obojson",
        output=os.path.join(TARGET_DIR, "phenio"),
        output_format="tsv",
        stream=False
    )

    tin = Transformer()
    s = TsvSource(tin)

    g = s.parse(filename=os.path.join(TARGET_DIR, "phenio_nodes.tsv"), format="tsv")

    nodes = {}
    for rec in g:
        if rec:
            if len(rec) != 4:
                nodes[rec[0]] = rec[1]

    n1 = nodes["GO:0051370"]
    assert n1["id"] == "GO:0051370"
    assert n1["name"] == "obsolete ZASP binding"
    assert n1["deprecated"] is True
