# KGX Specification

The KGX format is a serialization of Biolink Model compliant knowledge graphs. This document outlines the structure 
and organization of this format, detailing the required fields and their significance, along with examples in 
various formats.  KGX supports multiple serialization formats, including JSON, TSV, JSON Lines, and RDF Turtle.  Thus
KGX is both a format specification and a toolkit for serializing data conformant to Biolink Model in a variety of 
formats.

There are some notable initial design decisions for KGX that influence the behavior of the KGX toolkit:
* KGX is a serialization format for Biolink Model compliant knowledge graphs
* KGX is a flat file format that can be processed, subset, and exchanged easily
* Each node or edge is represented with all properties that describe it
* KGX prefers that all properties are valid Biolink Model properties, however it is designed to be lenient 
  and allow non-Biolink Model properties in an effort to be more inclusive of existing knowledge graphs and allow Biolink to evolve without breaking existing knowledge graphs.
* KGX is not a knowledge graph, but a serialization format for knowledge graphs
* KGX is not a knowledge graph model, but a serialization format for knowledge graph models.  It follows the Biolink Model.

```{toctree}
:maxdepth: 2
:hidden:

```

## Introduction

The KGX format is a serialization of Biolink Model compliant knowledge graphs. This specification defines 
how this format is structured and organized, describing the required fields and their significance, 
with examples in various formats.

## KGX Format

The KGX format represents Biolink Model compliant knowledge graphs as flat files that can be 
processed, subset, and exchanged easily. Each node or edge is represented with all properties that describe it.

### Node Record Elements

We refer to each serialization of a node as a Node record, with the following elements:

**Base-level Required Elements:**
- `id`: CURIE that uniquely identifies the node in the graph
- `category`: Multivalued list with values from the Biolink [NamedThing](https://biolink.github.io/biolink-model/NamedThing) hierarchy

**Optional Elements:**
- Biolink Model properties: `name`, `description`, `xref`, `provided_by`, etc.
- Note: Non-Biolink Model properties are allowed and won't violate the specification - this was an intentional design decision to be more inclusive of existing knowledge graphs and allow Biolink to evolve without breaking existing knowledge graphs.

### Edge Record Elements

Each serialization of an edge (Edge record) includes:

**Base-level Required Elements:**
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
- Note: Non-Biolink Model properties are allowed and won't violate the specification - this was an intentional design decision to be more inclusive of existing knowledge graphs and allow Biolink to evolve without breaking existing knowledge graphs.

When using KGX as a serialization framework (e.g. the "Transform" operations), note that KGX will try to add required properties with default values
when not provided by the user.  It will also assign Biolink categories to nodes if not provided by the user.  This is done to ensure that the resulting knowledge graph is Biolink Model compliant.

## Format Serializations

KGX supports multiple serialization formats for knowledge graphs.  KGX also has a very lightweight schema that imports
the Biolink Model and makes two key adjustments to Biolink's class hierarchy: it adds an is_a relationship between
"biolink:NamedThing" and "kgx:Node" and an is_a relationship between "biolink:Association" and "kgx:Edge"

For more information and examples of the KGX overlay schema, please see: [KGX Schema Generation](kgx_schema_generation.md).
For convenience, this is the base KGX schema:

```yaml
imports:
  - linkml:types
  - https://w3id.org/biolink/biolink-model

classes:
  KnowledgeGraph:
    description: A knowledge graph represented in KGX format
    slots:
      - nodes
      - edges
  
  Node:
    description: A node in a KGX graph, superclass for NamedThing
    slots:
      - id
      - name
      - description
      - category
      - xref
      - provided by
      # ... other node slots ...
    
  Edge:
    description: An edge in a KGX graph, superclass for Association
    slots:
      - id
      - subject
      - predicate
      - object
      - relation
      - category
      - provided by
      - knowledge source
      # ... other edge slots ...

slots:
  nodes:
    range: Node
    multivalued: true
    inlined: true
    
  edges:
    range: Edge
    multivalued: true
    inlined: true
```

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

The JSON Lines format provides a simple and efficient way to represent KGX data where each line contains a 
single JSON object representing either a node or an edge. This format combines the advantages of JSON 
(flexible schema, native support for lists and nested objects) with the streaming capabilities of line-oriented formats.

##### File Structure
- `{filename}_nodes.jsonl`: Contains one node per line, each as a complete JSON object
- `{filename}_edges.jsonl`: Contains one edge per line, each as a complete JSON object

##### Node Record Format

###### Required Properties
- `id` (string): A CURIE that uniquely identifies the node in the graph
- `category` (array of strings): List of Biolink categories for the node, from the [NamedThing](https://biolink.github.io/biolink-model/NamedThing) hierarchy

###### Common Optional Properties
- `name` (string): Human-readable name of the entity
- `description` (string): Human-readable description of the entity
- `provided_by` (array of strings): List of sources that provided this node
- `xref` (array of strings): List of database cross-references as CURIEs
- `synonym` (array of strings): List of alternative names for the entity

##### Edge Record Format

###### Required Properties
- `subject` (string): CURIE of the source node
- `predicate` (string): Biolink predicate representing the relationship type
- `object` (string): CURIE of the target node
- `knowledge_level` (string): Level of knowledge representation (observation, assertion, concept, statement) according to Biolink Model
- `agent_type` (string): Autonomous agents for edges (informational, computational, biochemical, biological) according to Biolink Model

###### Common Optional Properties
- `id` (string): Unique identifier for the edge, often a UUID
- `relation` (string): Relation CURIE from a formal relation ontology (e.g., RO)
- `category` (array of strings): List of Biolink association categories
- `knowledge_source` (array of strings): Sources of knowledge (deprecated: `provided_by`)
- `primary_knowledge_source` (array of strings): Primary knowledge sources
- `aggregator_knowledge_source` (array of strings): Knowledge aggregator sources
- `publications` (array of strings): List of publication CURIEs supporting the edge

#### Examples

**Node Example (nodes.jsonl)**:

Each line in a nodes.jsonl file represents a complete node record. Here are examples of different node types:

```json
{
  "id": "HGNC:11603",
  "name": "TBX4",
  "category": [
    "biolink:Gene"
  ]
}
```
```json
{
  "id": "MONDO:0005002",
  "name": "chronic obstructive pulmonary disease",
  "category": [
    "biolink:Disease"
  ]
}
```
```json
{
  "id": "CHEBI:15365",
  "name": "acetaminophen",
  "category": [
    "biolink:SmallMolecule",
    "biolink:ChemicalEntity"
  ]
}
```
```

In the actual jsonlines file, each record would be on a single line without comments and formatting:

```text
{"id":"HGNC:11603","name":"TBX4","category":["biolink:Gene"]}
{"id":"MONDO:0005002","name":"chronic obstructive pulmonary disease","category":["biolink:Disease"]}
{"id":"CHEBI:15365","name":"acetaminophen","category":["biolink:SmallMolecule","biolink:ChemicalEntity"]}
```

**Edge Example (edges.jsonl)**:

Each line in a jsonlines file represents a complete edge record. Here are examples of different edge types:

```json
{
  "id": "a8575c4e-61a6-428a-bf09-fcb3e8d1644d",
  "subject": "HGNC:11603",
  "object": "MONDO:0005002",
  "predicate": "biolink:related_to",
  "relation": "RO:0003304",
  "knowledge_level": "assertion",
  "agent_type": "computational"
}
```

```json
{
  "id": "urn:uuid:5b06e86f-d768-4cd9-ac27-abe31e95ab1e",
  "subject": "HGNC:11603",
  "predicate": "biolink:contributes_to",
  "object": "MONDO:0005002",
  "relation": "RO:0003304",
  "category": [
    "biolink:GeneToDiseaseAssociation"
  ],
  "primary_knowledge_source": [
    "infores:gwas-catalog"
  ],
  "publications": [
    "PMID:26634245",
    "PMID:26634244"
  ],
  "knowledge_level": "observation",
  "agent_type": "biological"
}
```

```json
{
  "id": "c7d632b4-6708-4296-9cfe-44bc586d32c8",
  "subject": "CHEBI:15365",
  "predicate": "biolink:affects",
  "object": "GO:0006915",
  "relation": "RO:0002434",
  "category": [
    "biolink:ChemicalToProcessAssociation"
  ],
  "primary_knowledge_source": [
    "infores:monarchinitiative"
  ],
  "aggregator_knowledge_source": [
    "infores:biolink-api"
  ],
  "publications": [
    "PMID:12345678"
  ],
  "knowledge_level": "assertion",
  "agent_type": "computational"
}
```

In the actual jsonlines file, each record would be on a single line without comments and formatting:

```text
{"id":"a8575c4e-61a6-428a-bf09-fcb3e8d1644d","subject":"HGNC:11603","object":"MONDO:0005002","predicate":"biolink:related_to","relation":"RO:0003304","knowledge_level":"assertion","agent_type":"computational"}
{"id":"urn:uuid:5b06e86f-d768-4cd9-ac27-abe31e95ab1e","subject":"HGNC:11603","predicate":"biolink:contributes_to","object":"MONDO:0005002","relation":"RO:0003304","category":["biolink:GeneToDiseaseAssociation"],"primary_knowledge_source":["infores:gwas-catalog"],"publications":["PMID:26634245","PMID:26634244"],"knowledge_level":"observation","agent_type":"biological"}
```

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

### Usage Notes
- All field values should follow the KGX specification and Biolink Model requirements
- Arrays should be represented as JSON arrays (not pipe-delimited strings)
- For large KGs, JSON Lines offers better streaming performance than monolithic JSON

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