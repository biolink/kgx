.. kgx documentation master file, created by
   sphinx-quickstart on Tue Jun 18 16:51:38 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Knowledge Graph Exchange documentation
=================================================

KGX is a utility library and set of command line tools for exchanging data in Knowledge Graphs (KGs).

The tooling here is partly generic but intended primarily for building the translator-knowledge-graph, and thus
expects KGs to be `BioLink Model <https://biolink.github.io/biolink-model/>`_ compliant.

The tool allows you to fetch (sub)graphs from one (or more) KG and create an entirely new KG.

The core data model is a Property Graph (PG), represented internally in Python using a networkx MultiDiGraph.

KGX supports Neo4j and RDF triple stores, along with other serialization formats such as TSV, CSV, JSON and TTL.


Contents
--------

.. toctree::
   :maxdepth: 2

   installation
   documentation
   examples
   cli_usage


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
