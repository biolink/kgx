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

   command_line_usage.md


Examples
--------

.. toctree::
   :maxdepth: 2

   examples

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
