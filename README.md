# Knowledge Graph Exchange

[![Python](https://img.shields.io/badge/python-3.7+-blue.svg)]()
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
 * CSV/TSV and JSON (see [associated data formats](./data-preparation.md) and [example script to load CSV/TSV to Neo4j](./examples/scripts/load_csv_to_neo4j.py))
 * Reasoner Standard API format
 * OBOGraph JSON format


KGX will also provide validation, to ensure the KGs are conformant to the Biolink Model: making sure nodes are
categorized using Biolink classes, edges are labeled using valid Biolink relationship types, and valid properties are used.

Internal representation is a property graph, specifically a networkx MultiDiGraph.

The structure of this graph is expected to conform to the Biolink Model standard, as specified in the [KGX format specification](specification/kgx-format.md).

In addition to the main code-base, KGX also provides a series of [command line operations](https://kgx.readthedocs.io/en/latest/examples.html#using-kgx-cli).


## Installation

The installation for KGX requires Python 3.7 or greater.


### Installation for users


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
python setup.py install
```


### Installation for developers

#### Setting up a development environment

To build directly from source, first clone the GitHub repository,

```bash
git clone https://github.com/biolink/kgx
cd kgx
```

Then install the necessary dependencies listed in ``requirements.txt``,

```bash
pip3 install -r requirements.txt
```


For convenience, make use of the `venv` module in Python3 to create a
lightweight virtual environment,

```
python3 -m venv env
source env/bin/activate

pip install -r requirements.txt
```

To install KGX you can do one of the following,

```bash
pip install .

# OR 

python setup.py install
```

### Setting up a testing environment

KGX has a suite of tests that rely on Docker containers to run Neo4j specific tests.

To set up the required containers, first install [Docker](https://docs.docker.com/get-docker/)
on your local machine.

Once Docker is up and running, run the following commands:

```bash
docker run -d --name kgx-neo4j-integration-test \
            -p 7474:7474 -p 7687:7687 \
            --env NEO4J_AUTH=neo4j/test  \
            neo4j:3.5.25
```

```bash
docker run -d --name kgx-neo4j-unit-test  \
            -p 8484:7474 -p 8888:7687 \
            --env NEO4J_AUTH=neo4j/test \
            neo4j:3.5.25
```


**Note:** Setting up the Neo4j container is optional. If there is no container set up
then the tests that rely on them are skipped.
