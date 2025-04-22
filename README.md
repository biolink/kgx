# Knowledge Graph Exchange


[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)]()
![Run tests](https://github.com/biolink/kgx/workflows/Run%20tests/badge.svg)[![Documentation Status](https://readthedocs.org/projects/kgx/badge/?version=latest)](https://kgx.readthedocs.io/en/latest/?badge=latest)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=biolink_kgx&metric=alert_status)](https://sonarcloud.io/dashboard?id=biolink_kgx)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=biolink_kgx&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=biolink_kgx)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=biolink_kgx&metric=coverage)](https://sonarcloud.io/dashboard?id=biolink_kgx)
[![PyPI](https://img.shields.io/pypi/v/kgx)](https://img.shields.io/pypi/v/kgx)
[![Docker](https://img.shields.io/static/v1?label=Docker&message=biolink/kgx:latest&color=orange&logo=docker)](https://hub.docker.com/r/biolink/kgx)

KGX (Knowledge Graph Exchange) is a Python library and set of command line utilities for exchanging
Knowledge Graphs (KGs) that conform to or are aligned to the [Biolink Model](https://biolink.github.io/biolink-model/).

The core datamodel is a [Property Graph](https://neo4j.com/developer/graph-database/) (PG), represented
internally in Python using a [networkx MultiDiGraph model](https://networkx.github.io/documentation/stable/reference/classes/generated/networkx.MultiDiGraph.edges.html).

KGX allows conversion to and from:

 * RDF serializations (read/write) and SPARQL endpoints (read)
 * Neo4j endpoints (read) or Neo4j dumps (write)
 * CSV/TSV and JSON (see [associated data formats](docs/data-preparation.md) and [example script to load CSV/TSV to Neo4j](./examples/scripts/load_csv_to_neo4j.py))
 * Reasoner Standard API format
 * OBOGraph JSON format

KGX will also provide validation, to ensure the KGs are conformant to the Biolink Model: making sure nodes are
categorized using Biolink classes, edges are labeled using valid Biolink relationship types, and valid properties are used.

Internal representation is a property graph, specifically a networkx MultiDiGraph.

The structure of this graph is expected to conform to the Biolink Model standard, as specified in the [KGX format specification](specification/kgx-format.md).

In addition to the main code-base, KGX also provides a series of [command line operations](https://kgx.readthedocs.io/en/latest/examples.html#using-kgx-cli).

### Example usage
Validate:
```bash
poetry run kgx validate -i tsv tests/resources/merge/test2_nodes.tsv tests/resources/merge/test2_edges.tsv
```

Merge:
```bash
poetry run kgx merge —merge-config tests/resources/test-merge.yaml 
```

Graph Summary:
```bash
poetry run kgx graph-summary -i tests/resources/graph_nodes.tsv  -o summary.txt
```

Transform:
```bash
poetry run kgx transform —transform-config tests/resources/test-transform-tsv-rdf.yaml
```

### Error Detection and Reporting

Non-redundant JSON-formatted structured error logging is now provided in KGX Transformer, Validator, GraphSummary and MetaKnowledgeGraph operations.  See the various unit tests for the general design pattern (using the Validator as an example here):

```python
from kgx.validator import Validator
from kgx.transformer import Transformer

Validator.set_biolink_model("2.11.0")

# Validator assumes the currently set Biolink Release
validator = Validator()

transformer = Transformer(stream=True)

transformer.transform(
    input_args = {
        "filename": [
            "graph_nodes.tsv",
            "graph_edges.tsv",
        ],
        "format": "tsv",
    },
    output_args={
        "format": "null"
    },
    inspector=validator,
)

# Both the Validator and the Transformer can independently capture errors

# The Validator, from the overall semantics of the graph...
# Here, we just report severe Errors from the Validator (no Warnings)
validator.write_report(open("validation_errors.json", "w"), "Error")

# The Transformer, from the syntax of the input files... 
# Here, we catch *all* Errors and Warnings (by not providing a filter)
transformer.write_report(open("input_errors.json", "w"))
```

The JSON error outputs will look something like this:

```json
{
    "ERROR": {
        "MISSING_EDGE_PROPERTY": {
            "Required edge property 'id' is missing": [
                "A:123->X:1",
                "B:456->Y:2"
            ],
            "Required edge property 'object' is missing": [
                "A:123->X:1"
            ],
            "Required edge property 'predicate' is missing": [
                "A:123->X:1"
            ],
            "Required edge property 'subject' is missing": [
                "A:123->X:1",
                "B:456->Y:2"
            ]
        }
    },
    "WARNING": {
        "DUPLICATE_NODE": {
          "Node 'id' duplicated in input data": [
            "MONDO:0010011",
            "REACT:R-HSA-5635838"
          ]
        }
    }
}

```

This system reduces the significant redundancies of earlier line-oriented KGX  logging text output files, in that graph entities with the same class of error are simply aggregated in lists of names/identifiers at the leaf level of the JSON structure.

The top level JSON tags originate from the `MessageLevel` class and the second level tags from the `ErrorType` class in the [error_detection](kgx/error_detection.py) module, while the third level messages are hard coded as `log_error` method messages in the code.  

It is likely that additional error conditions within KGX can be efficiently captured and reported in the future using this general framework.

## Installation

#### Installing from PyPI

KGX is available on PyPI and can be installed using
[pip](https://pip.pypa.io/en/stable/installing/) as follows,

```bash
pip install kgx
```

To install a particular version of KGX, be sure to specify the version number,

```bash
pip install kgx==0.5.0
```

#### Installing from GitHub

Clone the GitHub repository and then install,

```bash
git clone https://github.com/biolink/kgx
cd kgx
poetry install
```

### Setting up a testing environment for Neo4j

This release of KGX supports graph source and sink transactions with the 4.3 release of Neo4j.

KGX has a suite of tests that rely on Docker containers to run Neo4j specific tests.

To set up the required containers, first install [Docker](https://docs.docker.com/get-docker/)
on your local machine.

Once Docker is up and running, run the following commands:

```bash
docker run -d --rm --name kgx-neo4j-integration-test -p 7474:7474 -p 7687:7687 --env NEO4J_AUTH=neo4j/test neo4j:4.3
```

```bash
docker run -d --rm --name kgx-neo4j-unit-test -p 8484:7474 -p 8888:7687 --env NEO4J_AUTH=neo4j/test neo4j:4.3
```

**Note:** Setting up the Neo4j container is optional. If there is no container set up
then the tests that rely on them are skipped.
