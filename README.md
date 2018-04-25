# knowledge graph interchange

A utility library and set of command line tools for exchanging data in knowledge graphs.

The tooling here is partly generic but intended primarily for building
the
[translator-knowledge-graph](https://github.com/NCATS-Tangerine/translator-knowledge-graph).

For additional background see the [Translator Knowledge Graph Drive](http://bit.ly/tr-kg)

## Command Line Usage

Coming soon. For now see the [tests](tests/) for example

## Internal Representation

Internal representation is networkx MultiDiGraph which is a property graph.

The structure of this graph is expected to conform to the [tr-kg
standard](http://bit.ly/tr-kg-standard), briefly summarized here:

 * [Nodes](https://biolink.github.io/biolink-model/docs/NamedThing.html)
    * id : required
    * name : string
    * category : string. broad high level type. Corresponds to label in neo4j
    * extensible other properties, depending on 
 * [Edges](https://biolink.github.io/biolink-model/docs/Association.html) 
    * subject : required
    * predicate : required
    * object : required
    * extensible other fields

## Serialization/Deserialization

Intended to support

 - Generic Graph Formats
 - local or remote files
    - CSV
    - RDF (Monarch/OBAN style, ...)
    - GraphML
    - CX
 - remote store via query API
    - neo4j/bolt
    - RDF


## RDF

## Neo4J

Neo4j implements property graphs out the box. However, some
implementations use reification nodes. The transform should allow for
de-reification.

