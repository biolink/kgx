# Source

A Source can be implemented for any file, local, and/or remote store that can contains a graph. A Source is responsible
for reading nodes and edges from the graph.

A source must subclass `kgx.source.source.Source` class and must implement the following methods:
- `parse`
- `read_nodes`
- `read_edges`


**`parse` method**

- Responsible for parsing a graph from a file/store
- Must return a generator that iterates over list of node and edge records from the graph


**`read_nodes` method**

- Responsible for reading nodes from the file/store
- Must return a generator that iterates over list of node records
- Each node record must be a 2-tuple `(node_id, node_data)` where,
    - `node_id` is the node CURIE
    - `node_data` is a dictionary that represents the node properties


**`read_edges` method**

- Responsible for reading edges from the file/store
- Must return a generator that iterates over list of edge records
- Each edge record must be a 4-tuple `(subject_id, object_id, edge_key, edge_data)` where,
    -  `subject_id` is the subject node CURIE
    -  `object_id` is the object node CURIE
    -  `edge_key` is the unique key for the edge
    -  `edge_data` is a dictionary that represents the edge properties


## kgx.source.source

Base class for all Sources in KGX.


```{eval-rst}
.. automodule:: kgx.source.source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.graph_source

`GraphSource` is responsible for reading from an instance of `kgx.graph.base_graph.BaseGraph` and must use only
the methods exposed by `BaseGraph` to access the graph.


```{eval-rst}
.. automodule:: kgx.source.graph_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.tsv_source

`TsvSource` is responsible for reading from KGX formatted CSV or TSV using Pandas where every flat file is treated as a
Pandas DataFrame and from which data are read in chunks.

KGX expects two separate files - one for nodes and another for edges.  


```{eval-rst}
.. automodule:: kgx.source.tsv_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.json_source

`JsonSource` is responsible for reading data from a KGX formatted JSON using the [ijson](https://pypi.org/project/ijson/)
library, which allows for streaming data from the file.


```{eval-rst}
.. automodule:: kgx.source.json_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.jsonl_source

`JsonlSource` is responsible for reading data from a KGX formatted JSON Lines using the
[jsonlines](https://jsonlines.readthedocs.io/en/latest/) library.

KGX expects two separate JSON Lines files - one for nodes and another for edges.

## KGX JSON Lines Format Specification

The JSON Lines format provides an efficient way to represent KGX data where each line contains a single JSON object representing either a node or an edge. This format is ideal for streaming large graphs and combines the advantages of JSON with line-oriented processing.

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
  "category": ["biolink:Gene"]
},
{
  "id": "MONDO:0005002",
  "name": "chronic obstructive pulmonary disease",
  "category": ["biolink:Disease"]
},
{
  "id": "CHEBI:15365",
  "name": "acetaminophen",
  "category": ["biolink:SmallMolecule", "biolink:ChemicalEntity"]
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
},
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
},
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

### Reading JSON Lines with KGX
When using KGX to read JSON Lines files, the library will:
1. Parse each line as a complete JSON object
2. Validate required fields are present
3. Convert the data into the internal graph representation
4. Handle arrays properly as native Python lists (unlike TSV where lists are often pipe-delimited strings)


```{eval-rst}
.. automodule:: kgx.source.jsonl_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.trapi_source

`TrapiSource` is responsible for reading data from a [Translator Reasoner API](https://github.com/NCATSTranslator/ReasonerAPI)
formatted JSON.


```{eval-rst}
.. automodule:: kgx.source.trapi_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.obograph_source

`ObographSource` is responsible for reading data from [OBOGraphs](https://github.com/geneontology/obographs) in JSON.


```{eval-rst}
.. automodule:: kgx.source.obograph_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.sssom_source

`SssomSource` is responsible for reading data from an [SSSOM](https://github.com/mapping-commons/SSSOM)
formatted files.


```{eval-rst}
.. automodule:: kgx.source.sssom_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.neo_source

`NeoSource` is responsible for reading data from a local or remote Neo4j instance.


```{eval-rst}
.. automodule:: kgx.source.neo_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.rdf_source

`RdfSource` is responsible for reading data from RDF N-Triples.

This source makes use of a custom `kgx.parsers.ntriples_parser.CustomNTriplesParser` for parsing N-Triples,
which extends `rdflib.plugins.parsers.ntriples.W3CNTriplesParser`.

To ensure proper parsing of N-Triples and a relatively low memory footprint, it is recommended that the N-Triples
be sorted based on the subject IRIs.

```sh
sort -k 1,2 -t ' ' data.nt > data_sorted.nt
```


```{eval-rst}
.. automodule:: kgx.source.rdf_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.owl_source

`OwlSource` is responsible for parsing an [OWL](https://www.w3.org/TR/owl-features/) ontology.

When parsing an OWL, this source also adds [OwlStar](https://github.com/cmungall/owlstar) annotations
to certain OWL axioms. 


```{eval-rst}
.. automodule:: kgx.source.owl_source
   :members:
   :inherited-members:
   :show-inheritance:
```

