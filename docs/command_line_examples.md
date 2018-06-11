## Command Line Usage

Here we will walk through the basic work flow of using KGX from the command line. The files can be found in the [github repo](https://github.com/NCATS-Tangerine/kgx).

We'll assume that we're using a local instance of neo4j at `bolt://localhost:7687`, with the username `neo4j` and the password `password`.

### Validate
```
$ kgx validate tests/resources/semmed/cell.json tests/resources/semmed/gene.json tests/resources/semmed/protein.json
|Nodes|=395
|Edges|=300
ERROR:root:Item: umls_type Message: no such short form
ERROR:root:Item: labels Message: no such short form
ERROR:root:Item: xrefs Message: no such short form
ERROR:root:Item: pmids Message: no such short form
ERROR:root:Item: predicate Message: no such short form
ERROR:root:Item: n_pmids Message: no such short form
ERROR:root:Item: is_defined_by Message: no such short form
```

### Dump
Combine three files into one
```
$ kgx dump tests/resources/semmed/cell.json tests/resources/semmed/gene.json tests/resources/semmed/protein.json target/combined.json
|Nodes|=395
|Edges|=300
File created at: target/combined.json
```

### Neo4j

Uploading `combined.json` to a local neo4j instance
```
kgx neo4j-upload bolt://localhost:7687 neo4j password target/combined.json
|Nodes|=395
|Edges|=300
```

Downloading a subset of what we had uploaded. Running in debug mode so we can see the cypher queries
```
kgx --debug neo4j-download --properties object id UMLS:C1290952 --labels subject disease_or_phenotypic_feature bolt://localhost:7687 neo4j password target/neo4j-download.json
```

Downloading another subset, this time filtering on the edge label
```
kgx --debug neo4j-download --labels edge predisposes bolt://localhost:7687 neo4j password target/predisposes.json
```
