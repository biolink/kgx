Welcome to the KGX documentation
==================
KGX is a utility library and set of command line tools for exchanging data in Knowledge Graphs (KGs).

The tooling here is partly generic but intended primarily for building the translator-knowledge-graph,
and thus expects KGs to be [Biolink Model](https://biolink.github.io/biolink-model/) compliant.

The tool allows you to fetch (sub)graphs from one (or more) KG and create an entirely new KG.

The core data model is a Property Graph (PG), with the default representation using a networkx MultiDiGraph.

KGX supports Neo4j and RDF triple stores, along with other serialization formats such as
TSV, CSV, JSON, JSON Lines, OBOGraph JSON, SSSOM, RDF NT, and OWL.

Documentation
=============

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   kgx_format
   kgx_schema_generation
   reference/index
   transformation_and_validation_examples
   developer-guide
   data-preparation

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


