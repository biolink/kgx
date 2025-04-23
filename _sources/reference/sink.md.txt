# Sink

A Sink can be implemented for any file, local, and/or remote store to which a graph can be written to. A Sink is responsible for writing nodes and edges from a graph.

A Sink must subclass `kgx.sink.sink.Sink` class and must implement the following methods:
- `__init__`
- `write_nodes`
- `write_edges`
- `finalize`


## `__init__` method

The `__init__` method is used to instantiate a Sink with configurations required for writing to a store.
- In the case of files, the `__init__` method will take the `filename` and `format` as arguments
- In the case of a graph store like Neo4j, the `__init__` method will take the `uri`, `username`, and `password` as arguments.

The `__init__` method also has an optional `kwargs` argument which can be used to supply variable number of arguments to this method, depending on the requirements for the store for which the Sink is being implemented.


## `write_nodes` method

- Responsible for receiving a node record and writing to a file/store


## `write_edges` method

- Responsible for receiving an edge record and writing to a file/store


## `finalize` method

Any operation that needs to be performed after writing all the nodes and edges to a file/store must be defined in this method.

For example,
- `kgx.source.tsv_source.TsvSource` has a `finalize` method that closes the file handles and creates an archive, if compression is desired
- `kgx.source.neo_sink.NeoSink` has a `finalize` method that writes any cached node and edge records


## kgx.sink.sink

Base class for all Sinks in KGX.


```{eval-rst}
.. automodule:: kgx.sink.sink
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.sink.graph_sink

`GraphSink` is responsible for writing to an instance of `kgx.graph.base_graph.BaseGraph` and must use only
the methods exposed by `BaseGraph` to access the graph.


```{eval-rst}
.. automodule:: kgx.sink.graph_sink
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.sink.tsv_sink

`TsvSink` is responsible for writing a KGX formatted CSV or TSV using Pandas.

KGX writes two separate files - one for nodes and another for edges.


```{eval-rst}
.. automodule:: kgx.sink.tsv_sink
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.sink.json_sink

`JsonSink` is responsible for writing a KGX formatted JSON using the [jsonstreams](https://pypi.org/project/jsonstreams/)
library, which allows for streaming records to the file.


```{eval-rst}
.. automodule:: kgx.sink.json_sink
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.sink.jsonl_sink

`JsonlSink` is responsible for writing a KGX formatted JSON Lines using the
[jsonlines](https://jsonlines.readthedocs.io/en/latest/) library. 

KGX writes two separate JSON Lines files - one for nodes and another for edges.

## KGX JSON Lines Format Specification

The JSON Lines format provides a simple and efficient way to represent KGX data where each line contains a single JSON object representing either a node or an edge. This format combines the advantages of JSON (flexible schema, native support for lists and nested objects) with the streaming capabilities of line-oriented formats.

### File Structure
- `{filename}_nodes.jsonl`: Contains one node per line, each as a complete JSON object
- `{filename}_edges.jsonl`: Contains one edge per line, each as a complete JSON object

### Node Record Format

#### Required Properties
- `id` (string): A CURIE that uniquely identifies the node in the graph
- `category` (array of strings): List of Biolink categories for the node, from the [NamedThing](https://biolink.github.io/biolink-model/NamedThing) hierarchy

#### Common Optional Properties
- `name` (string): Human-readable name of the entity
- `description` (string): Human-readable description of the entity
- `provided_by` (array of strings): List of sources that provided this node
- `xref` (array of strings): List of database cross-references as CURIEs
- `synonym` (array of strings): List of alternative names for the entity

### Edge Record Format

#### Required Properties
- `subject` (string): CURIE of the source node
- `predicate` (string): Biolink predicate representing the relationship type
- `object` (string): CURIE of the target node
- `knowledge_level` (string): Level of knowledge representation (observation, assertion, concept, statement) according to Biolink Model
- `agent_type` (string): Autonomous agents for edges (informational, computational, biochemical, biological) according to Biolink Model

#### Common Optional Properties
- `id` (string): Unique identifier for the edge, often a UUID
- `relation` (string): Relation CURIE from a formal relation ontology (e.g., RO)
- `category` (array of strings): List of Biolink association categories
- `knowledge_source` (array of strings): Sources of knowledge (deprecated: `provided_by`)
- `primary_knowledge_source` (array of strings): Primary knowledge sources
- `aggregator_knowledge_source` (array of strings): Knowledge aggregator sources
- `publications` (array of strings): List of publication CURIEs supporting the edge

### Examples

**Node Example (nodes.jsonl)**:

Each line in a nodes.jsonl file represents a complete node record. Here are examples of different node types:

```json
{
  "id": "HGNC:11603",
  "name": "TBX4",
  "category": [
    "biolink:Gene"
  ]
},
{
  "id": "MONDO:0005002",
  "name": "chronic obstructive pulmonary disease",
  "category": [
    "biolink:Disease"
  ]
},
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
  "predicate": "biolink:contributes_to",
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
  "category": [
    "biolink:GeneToDiseaseAssociation"
  ],
  "primary_knowledge_source": [
    "infores:agr"
  ],
  "aggregator_knowledge_source": [
    "infores:monarchinitiative"
  ],
  "publications": [
    "PMID:26634245",
    "PMID:26634244"
  ],
  "knowledge_level": "manual_assertion",
  "agent_type": ""
}
```

```json
{
  "id": "c7d632b4-6708-4296-9cfe-44bc586d32c8",
  "subject": "CHEBI:15365",
  "predicate": "biolink:affects",
  "qualified_predicate": "biolink:causes",
  "object": "HGNC:11603",
  "object_aspect_qualifier": "biolink:expression",
  "object_direction_qualifier": "biolink:increased",
  "category": [
    "biolink:ChemicalAffectsGeneAssociation"
  ],
  "primary_knowledge_source": [
    "infores:ctd"
  ],
  "aggregator_knowledge_source": [
    "infores:monarchinitiative"
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

### Usage Notes
- All field values should follow the KGX specification and Biolink Model requirements
- Arrays should be represented as JSON arrays (not pipe-delimited strings)
- For large KGs, JSON Lines offers better streaming performance than monolithic JSON

```{eval-rst}
.. automodule:: kgx.sink.jsonl_sink
   :members:
   :inherited-members:
   :show-inheritance:
```


## kgx.sink.neo_sink

`NeoSink` is responsible for writing data to a local or remote Neo4j instance.


```{eval-rst}
.. automodule:: kgx.sink.neo_sink
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.sink.rdf_sink

`RdfSink` is responsible for writing data as RDF N-Triples.


```{eval-rst}
.. automodule:: kgx.sink.rdf_sink
   :members:
   :inherited-members:
   :show-inheritance:
```


## kgx.sink.parquet_sink

`ParquetSink` is responsible for writing data as Parquet table files.

KGX writes two separate files - one for nodes and another for edges.


```{eval-rst}
.. automodule:: kgx.sink.parquet_sink
   :members:
   :inherited-members:
   :show-inheritance:
```