# KGX Specification

The KGX format is a serialization of Biolink Model compliant knowledge graphs.

```{toctree}
:maxdepth: 2
:hidden:

```

## Introduction

The KGX format is a serialization of Biolink Model compliant knowledge graphs. This specification defines how this format is structured and organized, describing the required fields and their significance, with examples in various formats.

## KGX Format

The KGX format represents Biolink Model compliant knowledge graphs as flat files that can be processed, subset, and exchanged easily. Each node or edge is represented with all properties that describe it.

### Node Record Elements

We refer to each serialization of a node as a Node record, with the following elements:

**Required Elements:**
- `id`: CURIE that uniquely identifies the node in the graph
- `category`: Multivalued list with values from the Biolink [NamedThing](https://biolink.github.io/biolink-model/NamedThing) hierarchy

**Optional Elements:**
- Biolink Model properties: `name`, `description`, `xref`, `provided_by`, etc.
- Non-Biolink Model properties are allowed and won't violate the specification

### Edge Record Elements

Each serialization of an edge (Edge record) includes:

**Required Elements:**
- `subject`: ID of the source node
- `predicate`: Relationship type from Biolink [related_to](https://biolink.github.io/biolink-model/related_to) hierarchy
- `object`: ID of the target node
- `knowledge_level`: Level of knowledge representation (observation, assertion, concept, statement) according to Biolink Model
- `agent_type`: Autonomous agents for edges (informational, computational, biochemical, biological) according to Biolink Model

**Edge Provenance:**
- Use `knowledge_source` or its descendants (`primary_knowledge_source`, etc.)
- `publications`: List of publication CURIEs supporting the edge

**Optional Elements:**
- Biolink Model properties: `category`, `publications`, etc.
- Non-Biolink Model properties are allowed

## Format Serializations

KGX supports multiple serialization formats for knowledge graphs.

### KGX format as JSON

```json
{
    "nodes" : [
      {
        "id": "HGNC:11603",
        "name": "TBX4",
        "category": ["biolink:Gene"],
        "provided_by": ["infores:gwascatalog"]
      },
      {
        "id": "MONDO:0005002",
        "name": "chronic obstructive pulmonary disease",
        "category": ["biolink:Disease"],
        "provided_by": ["infores:gwascatalog"]
      }
    ],
    "edges" : [
      {
        "id": "urn:uuid:5b06e86f-d768-4cd9-ac27-abe31e95ab1e",
        "subject": "HGNC:11603",
        "predicate": "biolink:contributes_to",
        "object": "MONDO:0005002",
        "relation": "RO:0003304",
        "category": ["biolink:GeneToDiseaseAssociation"],
        "primary_knowledge_source": ["infores:gwascatalog"],
        "publications": ["PMID:26634245", "PMID:26634244"]
      }
    ]
}
```

### KGX format as TSV

KGX TSV format uses two files - one for nodes and another for edges.

**nodes.tsv**
```tsv
id	category	name	provided_by
HGNC:11603	biolink:NamedThing|biolink:BiologicalEntity|biolink:Gene	TBX4	infores:gwascatalog
MONDO:0005002	biolink:NamedThing|biolink:BiologicalEntity|biolink:DiseaseOrPhenotypicFeature|biolink:Disease	chronic obstructive pulmonary disease	infores:gwascatalog
```

**edges.tsv**
```tsv
id	subject	predicate	object	relation	primary_knowledge_source	category	publications
urn:uuid:5b06e86f-d768-4cd9-ac27-abe31e95ab1e	HGNC:11603	biolink:contributes_to	MONDO:0005002	RO:0003304	infores:gwascatalog	biolink:GeneToDiseaseAssociation	PMID:26634245|PMID:26634244
```

**Notes:**
- Multi-valued fields use pipe (`|`) as delimiter
- TSV can be sparse when some nodes have specialized properties
- Column ordering can be inconsistent for non-core properties

### KGX format as JSON Lines

Each line contains a single JSON object representing a node or edge.

**nodes.jsonl**
```
{"id":"HGNC:11603","name":"TBX4","category":["biolink:Gene"]}
{"id":"MONDO:0005002","name":"chronic obstructive pulmonary disease","category":["biolink:Disease"]}
{"id":"CHEBI:15365","name":"acetaminophen","category":["biolink:SmallMolecule","biolink:ChemicalEntity"]}
```

**edges.jsonl**
```
{"id":"urn:uuid:5b06e86f-d768-4cd9-ac27-abe31e95ab1e","subject":"HGNC:11603","predicate":"biolink:contributes_to","object":"MONDO:0005002","relation":"RO:0003304","category":["biolink:GeneToDiseaseAssociation"],"primary_knowledge_source":["infores:gwas-catalog"],"publications":["PMID:26634245","PMID:26634244"],"knowledge_level":"observation","agent_type":"biological"}
```

### KGX format as RDF Turtle

```ttl
@prefix OBO: <http://purl.obolibrary.org/obo/> .
@prefix biolink: <https://w3id.org/biolink/vocab/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<urn:uuid:5b06e86f-d768-4cd9-ac27-abe31e95ab1e> rdf:object OBO:MONDO_0005002 ;
    rdf:predicate biolink:contributes_to ;
    rdf:subject <http://identifiers.org/hgnc/11603> ;
    biolink:category biolink:GeneToDiseaseAssociation ;
    biolink:provided_by <https://archive.monarchinitiative.org/201806/gwascatalog> ;
    biolink:publications <http://www.ncbi.nlm.nih.gov/pubmed/26634244>,
        <http://www.ncbi.nlm.nih.gov/pubmed/26634245> ;
    biolink:relation OBO:RO_0003304 .

<http://identifiers.org/hgnc/11603> rdfs:label "TBX4"^^xsd:string ;
    biolink:category biolink:Gene ;
    biolink:contributes_to OBO:MONDO_0005002 ;
    biolink:provided_by <https://archive.monarchinitiative.org/201806/gwascatalog> .

OBO:MONDO_0005002 rdfs:label "chronic obstructive pulmonary disease"^^xsd:string ;
    biolink:category biolink:Disease ;
    biolink:primary_knowledge_source <https://archive.monarchinitiative.org/201806/gwascatalog> .
```