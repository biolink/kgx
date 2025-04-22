# KGX transformation and validation examples

There are two modes for using KGX:

- Using KGX as a module
- Using KGX CLI


## Using KGX as a module

KGX provides a variety of functionality that can be achieved by your script 
by importing KGX as a module and calling the relevant classes/methods as needed.

Examples on how to use the KGX as a module can be found in
[examples folder](https://github.com/biolink/kgx/tree/master/examples). Some additional insights on usage
may also be obtained by examination of the [tests folder](https://github.com/biolink/kgx/tree/master/tests).


## Using KGX CLI

The KGX CLI is a way of accessing KGX's functionality directly from the command line.

Currently, the CLI supports the following operations:

### graph-summary

Summarizes a graph and generate a YAML report regarding the composition of node and edge types in the graph.

```bash
    kgx graph-summary --input-format tsv \
                      --output graph_stats.yaml \
                      --report-type kgx-map \
                      --error-log graph_stats.err \
                      tests/resources/graph_nodes.tsv tests/resources/graph_edges.tsv
```

An alternate summary of a graph generates a TRAPI 1.*-compliant meta knowledge graph ("content metadata") JSON report:

```bash
    kgx graph-summary --input-format tsv \
                      --output content_metadata.json \
                      --report-type meta-knowledge-graph \
                      --error-log graph_stats.err \
                      tests/resources/graph_nodes.tsv tests/resources/graph_edges.tsv
```

Some basic validation is done during **graph-summary** operation, with detected errors reported on the `--error_log` (default: `stderr`).  For more complete graph validation,  the **validate** command (below) may be used.

### validate

Validate a graph for Biolink Model compliance and generate a report for nodes
and edges that are not compliant (if any).

```bash
    kgx validate --input-format tsv \
                 tests/resources/test_nodes.tsv tests/resources/test_edges.tsv
```


### neo4j-download

Download a (sub)graph from a local or remote Neo4j instance.

```bash
    kgx neo4j-download --uri http://localhost:7474 \
                       --username neo4j \
                       --password admin \
                       --output neo_graph_download \
                       --output-format tsv
```


### neo4j-upload

Upload a (sub)graph to a clean local or remote Neo4j instance.

**Note:** This operation expects the Neo4j instance to be empty. This operation 
does not support updating an existing Neo4j graph. Writing to an existing graph
may lead to side effects.

```bash
    kgx neo4j-upload --uri http://localhost:7474 \
                     --username neo4j \
                     --password admin \
                     --input-format tsv \
                     tests/resources/test_nodes.tsv tests/resources/test_edges.tsv
```


### transform

Transform a graph from one serialization to another (including a knowledge source InfoRes rewrite).

```bash
    kgx transform --input-format tsv \
                  --output test_graph.json \
                  --output-format json \
                  --knowledge-sources aggregator_knowledge_source "string,string database" \
                  --knowledge-sources aggregator_knowledge_source "go,gene ontology" \
                  tests/resources/graph_nodes.tsv tests/resources/graph_edges.tsv
```

Alternatively, you can also perform transformation driven by a YAML.

A sample of the merge configuration can be found [here](https://github.com/NCATS-Tangerine/kgx/blob/master/examples/sample-transform-config.yml).

```bash
    kgx transform --transform-config transform.yaml
```

### merge

Merge two (or more) graphs as defined by a YAML merge configuration.

A sample of the merge configuration can be found [here](https://github.com/NCATS-Tangerine/kgx/blob/master/examples/sample-merge-config.yml)

```bash
    kgx merge --merge-config merge.yaml
```
