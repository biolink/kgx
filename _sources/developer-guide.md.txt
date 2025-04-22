# KGX Developer Guide

This guide can be considered as a reference for developers keen on contributing to KGX.


## Contents

- [Architecture](#architecture)
- [Design Principles](#design-principles)
- [Conventions](#conventions)
- [Continuous Integration](#continuous-integration)
- [Releases](#releases)
- [Roadmap](#roadmap)


## Architecture

The current [1.x.x](https://github.com/biolink/kgx/tree/master) architecture is a major rewrite from KGX [0.x.x](https://github.com/biolink/kgx/tree/0.x.x). The main motivation for this rewrite was to,
- reduce complexity
- increase flexibility
- improve readability
- add ability to stream graphs

The rest of this guide will assume KGX [1.x.x](https://github.com/biolink/kgx/tree/master) as the canonical architecture.

## Design Principles

Following are certain principles to keep in mind when working on the KGX codebase - whether
modifying existing implementation or writing new ones.


### Source

A Source can be implemented for any file, local, and/or remote store that can contains a graph. A Source
is responsible for reading nodes and edges from the graph.

A source must subclass `kgx.source.source.Source` class and must implement the following methods:
- `parse`
- `read_nodes`
- `read_edges`


#### `parse` method

- Responsible for parsing a graph from a file/store
- Must return a generator that iterates over list of node and edge records from the graph



#### `read_nodes` method

- Responsible for reading nodes from the file/store
- Must return a generator that iterates over list of node records
- Each node record must be a 2-tuple `(node_id, node_data)` where,
    - `node_id` is the node CURIE
    - `node_data` is a dictionary that represents the node properties

#### `read_edges` method

- Responsible for reading edges from the file/store
- Must return a generator that iterates over list of edge records
- Each edge record must be a 4-tuple `(subject_id, object_id, edge_key, edge_data)` where,
    -  `subject_id` is the subject node CURIE
    -  `object_id` is the object node CURIE
    -  `edge_key` is the unique key for the edge
    -  `edge_data` is a dictionary that represents the edge properties


### Sink

A Sink can be implemented for any file, local, and/or remote store to which a graph can be written to. A Sink is
responsible for writing nodes and edges from a graph.

A Sink must subclass `kgx.sink.sink.Sink` class and must implement the following methods:
- `__init__`
- `write_nodes`
- `write_edges`
- `finalize`


#### `__init__` method

The `__init__` method is used to instantiate a Sink with configurations required for writing to a store.
- In the case of files, the `__init__` method will take the `filename` and `format` as arguments
- In the case of a graph store like Neo4j, the `__init__` method will take the `uri`, `username`, and `password` as arguments.

The `__init__` method also has an optional `kwargs` argument which can be used to supply variable number
of arguments to this method, depending on the requirements for the store for which the Sink is being implemented.


### `write_nodes` method

- Responsible for receiving a node record and writing to a file/store


### `write_edges` method

- Responsible for receiving an edge record and writing to a file/store


### `finalize` method

Any operation that needs to be performed after writing all the nodes and edges to a file/store must be defined in this method.

For example,
- `kgx.source.tsv_source.TsvSource` has a `finalize` method that closes the file handles and creates an archive, if compression is desired
- `kgx.source.neo_sink.NeoSink` has a `finalize` method that writes any cached node and edge records


### Transformer

The Transformer class is responsible for reading data from an instance of `kgx.source.source.Source` 
and writing to an instance of `kgx.sink.sink.Sink`.

The Transformer supports various scenarios of execution.

#### Scenario I

Read from a source and write to an intermediate `kgx.graph.base_graph.BaseGraph` instance.

```py
from kgx.transformer import Transformer

input_args = {'filename': ['graph_nodes.tsv', 'graph_edges.tsv'], 'format': 'tsv'}

t = Transformer()
t.transform(input_args=input_args)
```

And then save the graph from the intermediate graph to a desired sink.

```py
output_args = {'filename': 'graph.json', 'format': 'json'}
t.save(output_args=output_args)
```

#### Scenario II

Read from a source, write to an intermediate `kgx.graph.base_graph.BaseGraph` instance, and then write to the desired sink.

```py
from kgx.transformer import Transformer

input_args = {'filename': ['graph_nodes.tsv', 'graph_edges.tsv'], 'format': 'tsv'}
output_args = {'filename': 'graph.json', 'format': 'json'}

t = Transformer()
t.transform(input_args=input_args, output_args=output_args)
```

#### Scenario III

Stream from a source and write to a desired sink.

```py
from kgx.transformer import Transformer

input_args = {'filename': ['graph_nodes.tsv', 'graph_edges.tsv'], 'format': 'tsv'}
output_args = {'filename': 'graph.json', 'format': 'json'}

t = Transformer(stream=True)
t.transform(input_args=input_args, output_args=output_args)
```

#### Scenario IV

Stream from a source, compute some graph operations - e.g. `graph-summary`, `validate` or a custom inspector (see below) - on the input graph  and then throw the data away, into a 'null' format Sink.

```py
from typing import List
from kgx.transformer import Transformer
from kgx.utils.kgx_utils import GraphEntityType

input_args = {'filename': ['graph_nodes.tsv', 'graph_edges.tsv'], 'format': 'tsv'}
output_args = {'format': 'null'}

class TestInspector:
    def __init__(self):
        self._node_count = 0
        self._edge_count = 0

    def __call__(self, entity_type: GraphEntityType, rec: List):
        if entity_type == GraphEntityType.EDGE:
            self._edge_count += 1
        elif entity_type == GraphEntityType.NODE:
            self._node_count += 1
        else:
            raise RuntimeError("Unexpected GraphEntityType: " + str(entity_type))

    def get_node_count(self):
        return self._node_count

    def get_edge_count(self):
        return self._edge_count

inspector = TestInspector()

t = Transformer(stream=True)
t.transform(
  input_args=input_args,
  output_args=output_args,
  inspector=inspector
)

print(inspector.get_node_count())
print(inspector.get_edge_count())
```

### Utils

Any method that is used across the codebase must be placed in `kgx.utils`, unless those methods are bound methods
that need to rely on the state of a class.
- Any method that is generic and can be used across the codebase can be placed in `kgx.utils.kgx_utils`
- Any method that has to do with graph traversals can be placed in `kgx.utils.graph_utils`
- Any method that has to do with RDF specific functions can be placed in `kgx.utils.rdf_utils`


### Graph Operations

KGX also has a small collection of graph operations that can be applied to an instance of `kgx.graph.base_graph.BaseGraph`.

Every new graph operation must be implemented as its own separate submodule in `kgx.graph_operations`.

Every new graph operation must take an instance of `kgx.graph.base_graph.BaseGraph` as its first argument, followed by other arguments specific for that operation.

For more information, refer to the KGX documentation on [Graph Operations](https://kgx.readthedocs.io/en/latest/reference/graph_operations/index.html).


### Validator

KGX has a validator which checks whether a given graph is Biolink Model compliant.


For more information, refer to the KGX documentation on [Valdiator](https://kgx.readthedocs.io/en/latest/reference/validator.html).


### KGX CLI

The KGX Command Line Interface is built using the [Click](https://github.com/pallets/click) library.

The main entrypoint for CLI is `kgx.cli:cli`.

As a design choice, all CLI operations should be implemented in `kgx.cli.cli_utils` and exposed as wrappers in `kgx.cli`.

For more information, refer to the KGX documentation on [KGX CLI](https://kgx.readthedocs.io/en/latest/reference/cli/index.html).


## Conventions

The following section details the various conventions used throughout the codebase.


### Code formatting

The code formatting is periodically done using the [Black](https://github.com/psf/black) Python library.

```sh
black --skip-string-normalization --line-length 100 kgx
black --skip-string-normalization --line-length 100 tests
```


### Docstring guidelines

The KGX codebase makes use of [Pandas styled docstring](https://python-sprints.github.io/pandas/guide/pandas_docstring.html)
format for documenting classes and methods.

This format is also utilized by Sphinx documentation generator to autogenerate documentation for the codebase.


### Typing

Types are defined throughout the KGX codebase.

The typecheck is periodically done using the [Mypy](http://mypy-lang.org/) library.

```sh
mypy --strict-optional --ignore-missing-imports kgx/
```


## Continuous Integration

The KGX repository is configured to run tests on every commit and on every PR made to the `master` branch. These tests
are run via [GitHub Actions](https://github.com/biolink/kgx/blob/master/.github/workflows/run_tests.yml).

The KGX repository is also configured with [SonarCloud](https://sonarcloud.io/dashboard?id=biolink_kgx) that provides
a wide range of metrics that helps in determining the maintainability of the codebase. SonarCloud scans the repo
after every commit and PR to ensure that certain quality metrics are above satisfying limits. These metrics are
entirely for the sake of guiding better coding practices and in no way interferes with the ability to merge PRs.

If you are a core-developer of KGX then you should have admin access to the [KGX project on SonarCloud](https://sonarcloud.io/dashboard?id=biolink_kgx).


## Releases

KGX repository follows [Semantic Versioning guidelines](https://semver.org/) for versioning releases. 

There are currently two branches of KGX:
- The `master` branch is where the latest changes are merged into. All new releases on the `1.x.x` will be made
off of `master` branch.
- The `0.x.x` is the legacy implementation of KGX. This branch will be maintained where only bugs are addressed.
No new features will be added to this branch.

To make a new release of KGX, refer to [Release Instructions](../release-instructions.md).


If you are a core-developer of KGX then you should have push access to [KGX on PyPI](https://pypi.org/project/kgx/) and [KGX on DockerHub](https://hub.docker.com/repository/docker/biolink/kgx).




## Roadmap

KGX has several driver projects that guides its development.

It originally started out with addressing the needs of the [NCATS Biomedical Data Translator](https://ncats.nih.gov/translator) and has since found application in various other projects:
- [Monarch Initiative](https://monarchinitiative.org/)
- [KG-COVID-19](https://github.com/Knowledge-Graph-Hub/kg-covid-19)
- [KG Microbe](https://github.com/Knowledge-Graph-Hub/kg-microbe)
- [Knowledge Graph Hub](https://knowledge-graph-hub.github.io/)
- [OntoML](https://github.com/Knowledge-Graph-Hub/OntoML)
- [NEAT](https://github.com/Knowledge-Graph-Hub/NEAT/)
- [Runner](https://github.com/monarch-initiative/runner)

