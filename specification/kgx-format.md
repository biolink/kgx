# KGX Format

Author: Deepak Unni<br>
Date: 2021-01-12<br>
Version: 0.0.1<br>


> **Note:** This specification is constantly evolving based on the changes occuring in Biolink Model.

## Table of Contents

- [Introduction](#introduction)
- [KGX format](#kgx-format)
    - [Node Record](#node-record)
        - [Core Node Record Elements](#core-node-record-elements)
        - [Optional Node Record Elements](#optional-node-record-elements)
    - [Edge Record](#edge-record)
        - [Core Edge Record Elements](#core-edge-record-elements)
        - [Optional Edge Record Elements](#optional-edge-record-elements)
- [KGX format as JSON](#kgx-format-as-json)
- [KGX format as TSV](#kgx-format-as-tsv)
- [KGX format as RDF Turtle](#kgx-format-as-rdf-turtle)
        
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
- [id](https://biolink.github.io/biolink-model/docs/id)
- [category](https://biolink.github.io/biolink-model/docs/category)


##### id

The `id` element must have a value as a CURIE that uniquely identifies the node in the graph.


##### category

The `category` element is used to name the high level class in which this entity is categorized.
The element is a multivalued list which must have a value from the Biolink [NamedThing](https://biolink.github.io/biolink-model/docs/NamedThing) hierarchy.


#### Optional Node Record Elements


##### Biolink Model Elements

A node can have additional properties, as defined in the Biolink Model.

For example, [name](https://biolink.github.io/biolink-model/docs/name), [description](https://biolink.github.io/biolink-model/docs/description), and [xref](https://biolink.github.io/biolink-model/docs/xref).


##### Non-Biolink Model elements

A node can also have other properties that are not from the Biolink Model. While it is recommended that these properties are well represented in the model to begin with, having these as node properties will not violate the specification.


### Edge Record

We refer to each serialization of an edge as an Edge record.

Each edge record has a set of elements that describes the edge.

#### Core Edge Record Elements

There are 4 required elements for an Edge record:
- [subject](https://biolink.github.io/biolink-model/docs/subject)
- [predicate](https://biolink.github.io/biolink-model/docs/predicate)
- [object](https://biolink.github.io/biolink-model/docs/object)
- [relation](https://biolink.github.io/biolink-model/docs/relation)
- [category](https://biolink.github.io/biolink-model/docs/category)

##### subject

The `subject` element is used to refer to the source node in an edge/statement/assertion and must be the `id` of the source node.


##### predicate 

The `predicate` element is used to refer to the predicate/relationship that links a source node to a target node in an edge/statement/assertion.
This element must have a value from the Biolink [related_to](https://biolink.github.io/biolink-model/docs/related_to) hierarchy.


##### object

The `object` element is used to refer to the target node in an edge/statement/assertion and must be the `id` of the target node.


##### relation

The `relation` element is used to refer to a more specific relationship that further describes the edge/statement/assertion.
Usually, this is a term from the [Relations Ontology](http://www.obofoundry.org/ontology/ro.html) but other ontologies,
thesauri, and controlled vocabularies are allowed.

##### category

The category element is used to name the high level class in which this edge/statement/assertion is categorized.
The element is a multivalued list which must have a value from the Biolink [Association](https://biolink.github.io/biolink-model/docs/Association) hierarchy.


#### Optional Edge Record Elements

##### Biolink Model Elements

An edge can have additional properties, as defined in the Biolink Model.

For example, [provided_by](https://biolink.github.io/biolink-model/docs/provided_by) and [publications](https://biolink.github.io/biolink-model/docs/publications).

##### Non-Biolink Model Elements

An edge can also have other properties that are not from Biolink Model. While it is recommended that these properties are well represented in the model to begin with, having these as edge properties will not violate the specification.



## KGX format as a JSON

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
        "provided_by": "MonarchArchive:gwascatalog",
      },
      {
        "id": "MONDO:0005002",
        "name": "chronic obstructive pulmonary disease",
        "category": ["biolink:Disease"],
        "provided_by": "MonarchArchive:gwascatalog",
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
        "provided_by": "MonarchArchive:gwascatalog",
        "publications": ["PMID:26634245", "PMID:26634244"]
      }
    ]
}

```



## KGX format as TSV

The KGX TSV format is structured slightly different from the JSON in that there are two files - one for nodes and another for edges.

- `nodes.tsv`: each row corresponds to a Node record and each column corresponds to an element in the Node record (i.e. a node property).
- `edges.tsv`: each row corresponds to an Edge record and each column corresponds to an element in the Edge record (i.e. an edge property).

A sample KGX TSV that represents a graph with 2 nodes and 1 edge:

nodes.tsv
```tsv
id	category	name	provided_by
HGNC:11603	biolink:Gene	TBX4	MonarchArchive:gwascatalog
MONDO:0005002	biolink:Disease	chronic obstructive pulmonary disease	MonarchArchive:gwascatalog
```

edges.tsv
```tsv
id	subject	predicate	object	relation	provided_by	category	publications
urn:uuid:5b06e86f-d768-4cd9-ac27-abe31e95ab1e	HGNC:11603	biolink:contributes_to	MONDO:0005002	RO:0003304	MonarchArchive:gwascatalog	biolink:GeneToDiseaseAssociation	PMID:26634245|PMID:26634244
```

Few noted caveats of the TSV serialization:
- If you have a large graph where some nodes have certain specialized properties but most of them do not, then you end up with a sparse TSV for nodes with several columns that have no value.
- The order of the columns can be specified for core properties but not for other Biolink or non-Biolink properties. This leads to a mismatch in expecation on the ordering of columns in the TSV for nodes and/or edges.



## KGX format as RDF Turtle

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
    biolink:provided_by <https://archive.monarchinitiative.org/201806/gwascatalog> .

```
