import csv
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


def _parse_edge_annotation_edges():
    """
    Parse the edge-annotation fixture and key its edges by (subject, object).
    """
    t = Transformer()
    s = ObographSource(t)
    g = s.parse(
        os.path.join(RESOURCE_DIR, "obograph_edge_annotations.json"),
        knowledge_source="PHENIO",
    )
    return {(rec[0], rec[1]): rec[3] for rec in g if rec and len(rec) == 4}


def test_edge_annotations_derive_primary_knowledge_source():
    """
    ``prov:wasDerivedFrom`` names the contributing ontology as a release IRI;
    the file stem becomes an InfoRes CURIE. ``merged_import`` is overridden
    because the registry spells it differently.
    """
    edges = _parse_edge_annotation_edges()

    assert (
        edges[("MONDO:0000004", "MONDO:0002816")]["primary_knowledge_source"]
        == "infores:mondo"
    )
    assert edges[("CL:0000014", "CL:0000034")]["primary_knowledge_source"] == "infores:cl"
    assert (
        edges[("HsapDv:0000049", "HsapDv:0000198")]["primary_knowledge_source"]
        == "infores:hsapdv"
    )
    # imports/merged_import.owl is PHENIO's own merge of its imports
    assert (
        edges[("MONDO:0018642", "HP:0000988")]["primary_knowledge_source"]
        == "infores:phenio"
    )
    # knowledge_source from the transform config is left untouched
    assert "PHENIO" in edges[("MONDO:0000004", "MONDO:0002816")]["knowledge_source"]


def test_edge_annotations_route_source_by_value_shape():
    """
    MONDO overloads ``source`` with publications, cross-references, curator
    ORCIDs and reasoning markers. Each shape lands in a different Biolink slot,
    and every value is retained verbatim in ``_source``.
    """
    edges = _parse_edge_annotation_edges()

    # PMID and doi are publications, under both the dc:source and the
    # oboInOwl:source spelling
    assert edges[("MONDO:0000133", "MONDO:0100137")]["publications"] == [
        "PMID:29804726"
    ]
    assert edges[("MONDO:0018642", "GO:0007249")]["publications"] == [
        "doi:10.1038/ncomms6360"
    ]
    assert edges[("MONDO:0018642", "HP:0000988")]["publications"] == ["PMID:33340416"]

    # An ontology CURIE is a cross-reference
    e = edges[("MONDO:0000004", "MONDO:0002816")]
    assert e["xref"] == ["DOID:10493"]
    assert e["_source"] == ["DOID:10493", "MONDO:Inferred"]

    # Curation markers stay out of xref but remain in _source
    e = edges[("MONDO:0000290", "NCBITaxon:5763")]
    assert "xref" not in e
    assert e["_source"] == ["MONDO:Wikidata"]

    # A MONDO value with a numeric local part is a real term reference, not a
    # marker, even with an "-obsoleted" suffix
    e = edges[("MONDO:0005815", "MONDO:0021040")]
    assert e["xref"] == ["EFO:0007331", "MONDO:0018520-obsoleted"]


def test_edge_annotations_derive_knowledge_level_and_agent_type():
    """
    An ORCID means a human asserted the axiom; MONDO:Inferred/Entailed and
    oboInOwl:is_inferred mean a reasoner did. A human outranks the markers.
    """
    edges = _parse_edge_annotation_edges()

    e = edges[("MONDO:0000005", "MONDO:0100118")]
    assert e["agent_type"] == "manual_agent"
    assert e["knowledge_level"] == "knowledge_assertion"

    e = edges[("MONDO:0000004", "MONDO:0002816")]
    assert e["agent_type"] == "automated_agent"
    assert e["knowledge_level"] == "logical_entailment"

    e = edges[("CL:0000014", "CL:0000034")]
    assert e["agent_type"] == "automated_agent"
    assert e["knowledge_level"] == "logical_entailment"

    # MONDO:Redundant describes the axiom's position in the hierarchy, not how
    # it was arrived at, so the curator ORCID wins
    e = edges[("MONDO:0000009", "MONDO:0002243")]
    assert e["agent_type"] == "manual_agent"
    assert e["knowledge_level"] == "knowledge_assertion"

    # ... but with no ORCID, MONDO:Entailed does make it an entailment
    e = edges[("MONDO:0000009", "MONDO:0002245")]
    assert e["agent_type"] == "automated_agent"
    assert e["knowledge_level"] == "logical_entailment"


def test_edge_annotations_evidence_and_passthrough():
    """
    ``oboInOwl:evidence`` maps to ``has_evidence`` with its IRIs contracted.
    Annotations with no Biolink slot pass through under their own names rather
    than being dropped or dumped as a raw ``meta`` blob.
    """
    edges = _parse_edge_annotation_edges()

    e = edges[("CL:4304384", "CL:0011111")]
    assert e["has_evidence"] == ["NCBIGene:140919", "NCBIGene:14415"]

    # FBbt has no prefix-map entry, so it contracts to the generic OBO prefix
    e = edges[("OBO:FBbt_00000001", "UBERON:0000468")]
    assert e["mapping_justification"] == (
        "https://w3id.org/semapv/vocab/ManualMappingCuration"
    )
    assert e["mapping_cardinality"] == "n:1"
    assert e["mapping_date"] == "2024-07-12"
    assert e["subject_label"] == "organism"
    assert e["object_label"] == "multicellular organism"

    assert edges[("HsapDv:0000049", "HsapDv:0000198")]["notes"] == "debatable"

    # the raw meta dict is never emitted as an edge property
    for edge in edges.values():
        assert "meta" not in edge


def test_edge_annotations_catchall_keeps_column_count_bounded():
    """
    PHENIO's components carry a long tail of one-off annotation properties.
    Giving each its own column produced ~40 near-empty columns, so anything
    outside SSSOM and the alias table is folded into ``_annotations`` as
    predicate=value pairs rather than dropped.
    """
    t = Transformer()
    s = ObographSource(t)

    # SSSOM and aliased properties keep their own column
    assert (
        s.annotation_property_name("https://w3id.org/sssom/mapping_justification")
        == "mapping_justification"
    )
    assert (
        s.annotation_property_name(
            "http://www.geneontology.org/formats/oboInOwl#notes"
        )
        == "notes"
    )
    # one-off per-ontology properties do not
    for pred in (
        "http://www.geneontology.org/formats/oboInOwl#cardonality",
        "http://www.geneontology.org/formats/oboInOwl#todo",
        "http://www.geneontology.org/formats/oboInOwl#quote",
    ):
        assert s.annotation_property_name(pred) is None

    parsed = s.parse_edge_meta(
        {
            "basicPropertyValues": [
                {
                    "pred": "http://www.geneontology.org/formats/oboInOwl#todo",
                    "val": "check with FBbt",
                },
                {
                    "pred": "http://www.geneontology.org/formats/oboInOwl#notes",
                    "val": "debatable",
                },
            ]
        }
    )
    assert parsed["notes"] == "debatable"
    # the predicate is contracted; oboInOwl's registered prefix is OIO
    assert parsed["_annotations"] == ["OIO:todo=check with FBbt"]


def test_edge_annotations_normalize_hand_entered_curies():
    """
    MONDO contains hand-entered source values such as "PMID: 16322613"; the
    stray space would otherwise make the emitted CURIE unusable.
    """
    t = Transformer()
    s = ObographSource(t)

    assert s.normalize_reference("PMID: 16322613") == "PMID:16322613"
    assert s.normalize_reference("DOID: 2218") == "DOID:2218"
    assert s.normalize_reference("  OMIM:123  ") == "OMIM:123"
    # IRIs are contracted, not split on their scheme colon
    assert (
        s.normalize_reference("http://identifiers.org/ncbigene/14415")
        == "NCBIGene:14415"
    )
    assert (
        s.normalize_reference("https://github.com/obophenotype/cl/issues/589")
        == "https://github.com/obophenotype/cl/issues/589"
    )

    # a few MONDO axioms pack two references into one annotation value
    assert s.split_references("PMID:32181500, PMID:32905580") == [
        "PMID:32181500",
        "PMID:32905580",
    ]
    assert s.split_references("PMID:32644331 PMID:2541995") == [
        "PMID:32644331",
        "PMID:2541995",
    ]
    # ... but a single value with a stray space is not two references
    assert s.split_references("PMID: 16322613") == ["PMID:16322613"]
    assert s.split_references("DOID:2218") == ["DOID:2218"]


def test_edge_annotations_infores_map_override():
    """
    ``infores_map`` lets a caller correct the derived InfoRes ID for ontologies
    the registry names differently, without patching KGX.
    """
    t = Transformer()
    s = ObographSource(t)
    g = s.parse(
        os.path.join(RESOURCE_DIR, "obograph_edge_annotations.json"),
        infores_map={"cl": "cell-ontology"},
    )
    edges = {(rec[0], rec[1]): rec[3] for rec in g if rec and len(rec) == 4}

    assert (
        edges[("CL:0000014", "CL:0000034")]["primary_knowledge_source"]
        == "infores:cell-ontology"
    )
    # unrelated ontologies keep the derived default
    assert (
        edges[("MONDO:0000004", "MONDO:0002816")]["primary_knowledge_source"]
        == "infores:mondo"
    )


def test_infores_map_reaches_source_from_transform_config():
    """
    Regression: ``prepare_input_args`` builds an explicit whitelist of input
    arguments, so an ``infores_map`` in a transform config YAML was silently
    dropped and the source kept its mechanically derived IDs. Projects need
    this to reconcile KGX's derivation with the InfoRes IDs they already use.
    """
    resource_cwd = os.getcwd()
    try:
        os.chdir(RESOURCE_DIR)
        transform(inputs=None, transform_config="obograph_infores_map.yaml")
        edges_file = os.path.join(
            TARGET_DIR, "obograph_infores_map", "InforesMap_edges.tsv"
        )
        assert os.path.isfile(edges_file)
        sources = set()
        with open(edges_file) as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                if row.get("primary_knowledge_source"):
                    sources.add(row["primary_knowledge_source"])
    finally:
        os.chdir(resource_cwd)

    # both mapped stems are rewritten ...
    assert "infores:cell-ontology" in sources
    assert "infores:HsapDv" in sources
    assert "infores:cl" not in sources
    assert "infores:hsapdv" not in sources
    # ... and an unmapped stem keeps the mechanical derivation
    assert "infores:mondo" in sources


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
