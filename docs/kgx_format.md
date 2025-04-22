# KGX Data Distribution Format Specification

The KGX format is a serialization of Biolink Model compliant knowledge graphs.

```{toctree}
:maxdepth: 2
:hidden:

```

## Introduction

The KGX format is a serialization of Biolink Model compliant knowledge graphs.

This specification aims at defining how this format is structured and organized, describing the required fields and their significance, followed by examples of the KGX format in TSV, JSON, and RDF.


## KGX format

The KGX format came to be as a way of representing Biolink Model compliant knowledge graphs as flat files that can be processed, subsetted, and exchanged easily. The simplest way of thinking of the format is to think how one would expect a graph (for example, in Neo4j) to be seralized into a file.

Each node (or edge) is represented with all of its properties that further describes the node (or edge).


### Node Record

We refer to each serialization of a node as a Node record.

Each node record has a set of elements that describes the node.

#### Core Node Record Elements

There are 2 required elements for a Node record:
- [id](https://biolink.github.io/biolink-model/id)
- [category](https://biolink.github.io/biolink-model/category)


##### id

The `id` element must have a value as a CURIE that uniquely identifies the node in the graph.


##### category

The `category` element is used to name the high level class in which this entity is categorized.
The element is a multivalued list which must have a value from the Biolink [NamedThing](https://biolink.github.io/biolink-model/NamedThing) hierarchy.


#### Optional Node Record Elements


##### Biolink Model Elements

A node can have additional properties, as defined in the Biolink Model.

For example, [name](https://biolink.github.io/biolink-model/name), [description](https://biolink.github.io/biolink-model/description), and [xref](https://biolink.github.io/biolink-model/xref).  Nodes can also optionally have a [`provided_by`](https://biolink.github.io/biolink-model/provided_by) property.


##### Non-Biolink Model elements

A node can also have other properties that are not from the Biolink Model. While it is recommended that these properties are well represented in the model to begin with, having these as node properties will not violate the specification.


### Edge Record

We refer to each serialization of an edge as an Edge record.

Each edge record has a set of elements that describes the edge.

#### Core Edge Record Elements

There are 3 required elements for an Edge record:
- [subject](https://biolink.github.io/biolink-model/subject)
- [predicate](https://biolink.github.io/biolink-model/predicate)
- [object](https://biolink.github.io/biolink-model/object)

##### subject

The `subject` element is used to refer to the source node in an edge/statement/assertion and must be the `id` of the source node.


##### predicate 

The `predicate` element is used to refer to the predicate/relationship that links a source node to a target node in an edge/statement/assertion.
This element must have a value from the Biolink [related_to](https://biolink.github.io/biolink-model/related_to) hierarchy.


##### object

The `object` element is used to refer to the target node in an edge/statement/assertion and must be the `id` of the target node.


#### Edge Provenance

Edge provenance, if specified, should be specified by one of a fixed set of elements.  The original [`provided_by`](https://biolink.github.io/biolink-model/provided_by) property is now deprecated for edges in favor of the Biolink Model 2.0 defined [`knowledge_source`](https://biolink.github.io/biolink-model/knowledge_source) association slot or one of its descendents - [`aggregator_knowledge_source`](https://biolink.github.io/biolink-model/aggregator_knowledge_source),  [`primary_knowledge_source`](https://biolink.github.io/biolink-model/primary_knowledge_source) and [`original_knowledge_source`](https://biolink.github.io/biolink-model/original_knowledge_source) - or the related association slot, [`supporting_data_source`](https://biolink.github.io/biolink-model/supporting_data_source).


#### Optional Edge Record Elements

##### Biolink Model Elements

An edge can have additional properties, as defined in the Biolink Model.

For example, [category](https://biolink.github.io/biolink-model/category) and [publications](https://biolink.github.io/biolink-model/publications).


##### Non-Biolink Model Elements

An edge can also have other properties that are not from Biolink Model. While it is recommended that these properties are well represented in the model to begin with, having these as edge properties will not violate the specification.


## Format Serializations

KGX supports multiple serialization formats for knowledge graphs, each with its own structure and characteristics. Below are the different serialization formats supported by KGX.

### KGX format as JSON

The structure of the KGX JSON format is as follows:

```json
{
    "nodes": [],
    "edges": []
}
```


A sample KGX JSON that represents a graph with 2 nodes and 1 edge:

```json
{
    "nodes" : [
      {
        "id": "HGNC:11603",
        "name": "TBX4",
        "category": ["biolink:Gene"],
        "provided_by": ["MonarchArchive:gwascatalog"],
      },
      {
        "id": "MONDO:0005002",
        "name": "chronic obstructive pulmonary disease",
        "category": ["biolink:Disease"],
        "provided_by": ["MonarchArchive:gwascatalog"],
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
        "primary_knowledge_source": ["MonarchArchive:gwascatalog"],
        "publications": ["PMID:26634245", "PMID:26634244"]
      }
    ]
}

```

### KGX format as TSV

The KGX TSV format is structured slightly different from the JSON in that there are two files - one for nodes and another for edges.

- `nodes.tsv`: each row corresponds to a Node record and each column corresponds to an element in the Node record (i.e. a node property).
- `edges.tsv`: each row corresponds to an Edge record and each column corresponds to an element in the Edge record (i.e. an edge property).

A sample KGX TSV that represents a graph with 2 nodes and 1 edge:

nodes.tsv
```tsv
id	category	name	provided_by
HGNC:11603	biolink:NamedThing|biolink:BiologicalEntity|biolink:Gene	TBX4	MonarchArchive:gwascatalog
MONDO:0005002	biolink:NamedThing|biolink:BiologicalEntity|biolink:DiseaseOrPhenotypicFeature|biolink:Disease	chronic obstructive pulmonary disease	MonarchArchive:gwascatalog
```

edges.tsv
```tsv
id	subject	predicate	object	relation	primary_knowledge_source	category	publications
urn:uuid:5b06e86f-d768-4cd9-ac27-abe31e95ab1e	HGNC:11603	biolink:contributes_to	MONDO:0005002	RO:0003304	MonarchArchive:gwascatalog	biolink:GeneToDiseaseAssociation	PMID:26634245|PMID:26634244
```

Few noted caveats of the TSV serialization:
- If you have a large graph where some nodes have certain specialized properties but most of them do not, then you end up with a sparse TSV for nodes with several columns that have no value.
- The order of the columns can be specified for core properties but not for other Biolink or non-Biolink properties. This leads to a mismatch in expectation on the ordering of columns in the TSV for nodes and/or edges.
- Fields that accept lists of value - e.g. the above fields for `category` (which in the Biolink Model may contain all the ancestors category classes of the most specific category, as noted in the above example with `biolink:Gene` and `biolink:Disease`) and `publications` - have values typically a list of values delimited by a Unix pipe ('|') character, unless otherwise programmatically overridden, using an available `list_delimiter` parameter, during TSV _source_ or _sink_ data parsing by the KGX software tool)


### KGX format as JSON Lines

The JSON Lines format provides a simple and efficient way to represent KGX data where each line contains a single JSON object representing either a node or an edge. This format combines the advantages of JSON (flexible schema, native support for lists and nested objects) with the streaming capabilities of line-oriented formats.

#### File Structure
- `{filename}_nodes.jsonl`: Contains one node per line, each as a complete JSON object
- `{filename}_edges.jsonl`: Contains one edge per line, each as a complete JSON object

#### Node Record Format

##### Required Properties
- `id` (string): A CURIE that uniquely identifies the node in the graph
- `category` (array of strings): List of Biolink categories for the node, from the [NamedThing](https://biolink.github.io/biolink-model/NamedThing) hierarchy

##### Common Optional Properties
- `name` (string): Human-readable name of the entity
- `description` (string): Human-readable description of the entity
- `provided_by` (array of strings): List of sources that provided this node
- `xref` (array of strings): List of database cross-references as CURIEs
- `synonym` (array of strings): List of alternative names for the entity

#### Edge Record Format

##### Required Properties
- `subject` (string): CURIE of the source node
- `predicate` (string): Biolink predicate representing the relationship type
- `object` (string): CURIE of the target node
- `knowledge_level` (string): Level of knowledge representation (observation, assertion, concept, statement) according to Biolink Model
- `agent_type` (string): Autonomous agents for edges (informational, computational, biochemical, biological) according to Biolink Model

##### Common Optional Properties
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
"category": ["biolink:GeneToDiseaseAssociation"],
"primary_knowledge_source": ["infores:gwas-catalog"],
"publications": ["PMID:26634245", "PMID:26634244"],
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
"category": ["biolink:ChemicalToProcessAssociation"],
"primary_knowledge_source": ["infores:monarchinitiative"],
"aggregator_knowledge_source": ["infores:biolink-api"],
"publications": ["PMID:12345678"],
"knowledge_level": "assertion",
"agent_type": "computational"
}
```

In the actual jsonlines file, each record would be on a single line without comments and formatting:

```text
{"id":"a8575c4e-61a6-428a-bf09-fcb3e8d1644d","subject":"HGNC:11603","object":"MONDO:0005002","predicate":"biolink:related_to","relation":"RO:0003304","knowledge_level":"assertion","agent_type":"computational"}
{"id":"urn:uuid:5b06e86f-d768-4cd9-ac27-abe31e95ab1e","subject":"HGNC:11603","predicate":"biolink:contributes_to","object":"MONDO:0005002","relation":"RO:0003304","category":["biolink:GeneToDiseaseAssociation"],"primary_knowledge_source":["infores:gwas-catalog"],"publications":["PMID:26634245","PMID:26634244"],"knowledge_level":"observation","agent_type":"biological"}
```

#### Usage Notes
- All field values should follow the KGX specification and Biolink Model requirements
- Arrays should be represented as JSON arrays (not pipe-delimited strings)
- For large KGs, JSON Lines offers better streaming performance than monolithic JSON

### KGX format as RDF Turtle

A sample KGX RDF Turtle (TTL) that represents a graph with 2 nodes and 1 edge:

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