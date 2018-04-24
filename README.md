## knowledge graph interchange

A utility library and set of command line tools for exchanging data in knowledge graphs

Intended to support

 - local or remote files
    - CSV
    - RDF (Monarch/OBAN style, ...)
    - GraphML
    - CX
 - remote store via query API
    - neo4j/bolt
    - RDF

Internal representation is networkx MultiDiGraph which is a property graph.

The structure of this graph is expected to conform to

 * [Nodes](https://biolink.github.io/biolink-model/docs/NamedThing.html)
    * id : required
    * name : string
    
 * [Edges](https://biolink.github.io/biolink-model/docs/Association.html) 
