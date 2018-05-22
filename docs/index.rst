.. Knowledge Graph Exchange documentation master file, created by
   sphinx-quickstart on Fri May 18 17:03:18 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Knowledge Graph Exchange's documentation!
====================================================

A utility library and set of command line tools for exchanging data in knowledge graphs.

The tooling here is partly generic but intended primarily for building the translator-knowledge-graph.


Installation
------------

.. code-block:: console

   pip3 install -r requirements.txt
   python3 setup.py install

The installation requires Python 3.

For convenience, make use of the venv module in Python 3 to create a lightweight virtual environment:

.. code-block:: console

   python3 -m venv env
   source env/bin/activate

   pip install -r requirements.txt
   python setup.py install


Documentation
-------------

.. toctree::
   :maxdepth: 2


Examples
--------

.. toctree::
   :maxdepth: 2

   examples

Command Line Usage
------------------

There is a `kgx` command line utility that allows for import, export and transforming a Knowledge Graph.

.. code-block:: console

   Usage: kgx dump [OPTIONS] [INPUT]... OUTPUT

   Transforms a knowledge graph from one representation to another
   INPUT  : any number of files or endpoints
   OUTPUT : the output file

   Options:
      --input-type TEXT   Extention type of input files: ttl, json, csv, rq, tsv, graphml, sxpr
      --output-type TEXT  Extention type of output files: ttl, json, csv, rq, tsv, graphml, sxpr
      --help              Show this message and exit.

CSV/TSV representation require two files, one that represents the vertex set and one for the edge set.
JSON, TTL, SXPR, and GRAPHML files represent a whole graph in a single file. For this reason when creating CSV/TSV
representation we will zip the resulting files in a .tar file.

The format will be inferred from the file extention. But if this cannot be done then the `--input-type` and
`--output-type` flags are useful to tell the program what formats to use. Currently not all conversions are supported.

Here are some examples that mirror the tests:

.. code-block:: console

   $ kgx dump --output-type=csv tests/resources/x1n.csv tests/resources/x1e.csv target/x1out
   File created at: target/x1out.tar
   $ kgx dump tests/resources/x1n.csv tests/resources/x1e.csv target/x1n.graphml
   File created at: target/x1n.graphml
   $ kgx dump tests/resources/monarch/biogrid_test.ttl target/bgcopy.csv
   File created at: target/bgcopy.csv.tar
   $ kgx dump tests/resources/monarch/biogrid_test.ttl target/x1n.graphml
   File created at: target/x1n.graphml
   $ kgx dump tests/resources/monarch/biogrid_test.ttl target/x1n.json
   File created at: target/x1n.json


Internal Representation
-----------------------

Internal representation is networkx MultiDiGraph which is a property graph.

The structure of this graph is expected to conform to the `tr-kg <http://bit.ly/tr-kg-standard>`_ standard, briefly summarized here:

**Nodes**
   * `id` : required
   * `name` : string
   * `category` : string. broad high level type. Corresponds to label in neo4j
   * extensible other properties, depending on

**Edges**
   * `subject` : required
   * `predicate` : required
   * `object` : required
   * extensible other fields


Serialization/Deserialization
-----------------------------

Intended to support,

   * Generic Graph Formats
   * local or remote files
      * CSV
      * TSV (such as the RKB adapted data loading formats)
      * RDF (Monarch/OBAN style, ...)
      * GraphML
      * CX
   * remote store via query API
   * Neo4j/bolt
   * RDF

RDF
---

Neo4j
-----

Neo4j implements property graphs out the box. However, some implementations use reification nodes.
The transform should allow for de-reification.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
