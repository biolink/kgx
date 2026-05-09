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
    # Was 205 prior to the predicate-mapping fix: two distinct relations
    # (BFO:0000050 and RO:0002211) between GO:0007165 and GO:0008150 were both
    # silently demoted to biolink:related_to and produced colliding deterministic
    # edge ids, conflating them in the dict. With BFO:0000050 now correctly
    # mapped to biolink:part_of, all three edges are kept distinct.
    assert len(edges) == 206

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
    assert len(n1["related_synonym"]) == 3
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
    # Was 205 prior to the predicate-mapping fix: two distinct relations
    # (BFO:0000050 and RO:0002211) between GO:0007165 and GO:0008150 were both
    # silently demoted to biolink:related_to and produced colliding deterministic
    # edge ids, conflating them in the dict. With BFO:0000050 now correctly
    # mapped to biolink:part_of, all three edges are kept distinct.
    assert len(edges) == 206

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


def test_identifiers_org_uris_contracted_via_transform():
    """
    Regression: when running through ``transform()``, the prefix manager's
    ~600 default JSON-LD context mappings used to be wiped out by
    ``TsvSource.set_prefix_map({})``, leaving non-OBO IRIs uncontracted in the
    TSV output. After the fix, identifiers.org/{hgnc,ncbigene} URIs land as
    proper CURIEs.
    """
    output_basename = os.path.join(TARGET_DIR, "obograph_curie_and_predicate")
    transform(
        inputs=[os.path.join(RESOURCE_DIR, "obograph_curie_and_predicate.json")],
        input_format="obojson",
        output=output_basename,
        output_format="tsv",
        stream=False,
    )

    tin = Transformer()
    g = TsvSource(tin).parse(
        filename=output_basename + "_nodes.tsv", format="tsv"
    )
    nodes = {}
    for rec in g:
        if rec and len(rec) != 4:
            nodes[rec[0]] = rec[1]

    # Node ids are CURIEs, not IRIs. The original IRI is preserved in `iri`.
    assert "NCBIGene:698782" in nodes
    assert nodes["NCBIGene:698782"]["iri"] == "http://identifiers.org/ncbigene/698782"
    assert "HGNC:5" in nodes
    assert nodes["HGNC:5"]["iri"] == "http://identifiers.org/hgnc/5"


def test_obograph_predicate_mapping_uses_curie():
    """
    Regression: ``ObographSource.read_edge`` used to pass the IRI to
    ``bmt.Toolkit.get_element_by_mapping``, which only recognizes CURIE form.
    The lookup silently returned None and well-mapped RO predicates fell
    through to the catch-all biolink:related_to. After the fix, RO:0002162
    resolves to its proper biolink slot (in_taxon).
    """
    t = Transformer()
    s = ObographSource(t)
    g = s.parse(os.path.join(RESOURCE_DIR, "obograph_curie_and_predicate.json"))

    edges = []
    for rec in g:
        if rec and len(rec) == 4:
            edges.append(rec[3])

    taxon_edges = [e for e in edges if e.get("relation") == "RO:0002162"]
    assert len(taxon_edges) == 2
    for e in taxon_edges:
        assert e["predicate"] == "biolink:in_taxon", (
            f"expected biolink:in_taxon for RO:0002162, got {e['predicate']!r}"
        )


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
