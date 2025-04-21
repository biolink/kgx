# Source

A Source can be implemented for any file, local, and/or remote store that can contains a graph. A Source is responsible for reading nodes and edges from the graph.

A source must subclass `kgx.source.source.Source` class and must implement the following methods:
- `parse`
- `read_nodes`
- `read_edges`


**`parse` method**

- Responsible for parsing a graph from a file/store
- Must return a generator that iterates over list of node and edge records from the graph


**`read_nodes` method**

- Responsible for reading nodes from the file/store
- Must return a generator that iterates over list of node records
- Each node record must be a 2-tuple `(node_id, node_data)` where,
    - `node_id` is the node CURIE
    - `node_data` is a dictionary that represents the node properties


**`read_edges` method**

- Responsible for reading edges from the file/store
- Must return a generator that iterates over list of edge records
- Each edge record must be a 4-tuple `(subject_id, object_id, edge_key, edge_data)` where,
    -  `subject_id` is the subject node CURIE
    -  `object_id` is the object node CURIE
    -  `edge_key` is the unique key for the edge
    -  `edge_data` is a dictionary that represents the edge properties


## kgx.source.source

Base class for all Sources in KGX.


```eval_rst
.. automodule:: kgx.source.source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.graph_source

`GraphSource` is responsible for reading from an instance of `kgx.graph.base_graph.BaseGraph` and must use only
the methods exposed by `BaseGraph` to access the graph.


```eval_rst
.. automodule:: kgx.source.graph_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.tsv_source

`TsvSource` is responsible for reading from KGX formatted CSV or TSV using Pandas where every flat file is treated as a
Pandas DataFrame and from which data are read in chunks.

KGX expects two separate files - one for nodes and another for edges.  


```eval_rst
.. automodule:: kgx.source.tsv_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.json_source

`JsonSource` is responsible for reading data from a KGX formatted JSON using the [ijson](https://pypi.org/project/ijson/)
library, which allows for streaming data from the file.


```eval_rst
.. automodule:: kgx.source.json_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.jsonl_source

`JsonlSource` is responsible for reading data from a KGX formatted JSON Lines using the
[jsonlines](https://jsonlines.readthedocs.io/en/latest/) library.

KGX expects two separate JSON Lines files - one for nodes and another for edges.  


```eval_rst
.. automodule:: kgx.source.jsonl_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.trapi_source

`TrapiSource` is responsible for reading data from a [Translator Reasoner API](https://github.com/NCATSTranslator/ReasonerAPI)
formatted JSON.


```eval_rst
.. automodule:: kgx.source.trapi_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.obograph_source

`ObographSource` is responsible for reading data from [OBOGraphs](https://github.com/geneontology/obographs) in JSON.


```eval_rst
.. automodule:: kgx.source.obograph_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.sssom_source

`SssomSource` is responsible for reading data from an [SSSOM](https://github.com/mapping-commons/SSSOM)
formatted files.


```eval_rst
.. automodule:: kgx.source.sssom_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.neo_source

`NeoSource` is responsible for reading data from a local or remote Neo4j instance.


```eval_rst
.. automodule:: kgx.source.neo_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.rdf_source

`RdfSource` is responsible for reading data from RDF N-Triples.

This source makes use of a custom `kgx.parsers.ntriples_parser.CustomNTriplesParser` for parsing N-Triples,
which extends `rdflib.plugins.parsers.ntriples.W3CNTriplesParser`.

To ensure proper parsing of N-Triples and a relatively low memory footprint, it is recommended that the N-Triples
be sorted based on the subject IRIs.

```sh
sort -k 1,2 -t ' ' data.nt > data_sorted.nt
```


```eval_rst
.. automodule:: kgx.source.rdf_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.owl_source

`OwlSource` is responsible for parsing an [OWL](https://www.w3.org/TR/owl-features/) ontology.

When parsing an OWL, this source also adds [OwlStar](https://github.com/cmungall/owlstar) annotations
to certain OWL axioms. 


```eval_rst
.. automodule:: kgx.source.owl_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.sparql_source

`SparqlSource` has yet to be implemented.

In principle, `SparqlSource` should be able to read data from a local or remote SPARQL endpoint. 


```eval_rst
.. automodule:: kgx.source.sparql_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.trapi_source

`TrapiSource` is responsible for reading data from TRAPI (Translator Reasoner API) JSON format and converting
it into KGX format.  For more information on TRAPI source, see the TRAPI source [doc](trapi_source.md)  .

```eval_rst
.. automodule:: kgx.source.trapi_source
   :members:
   :inherited-members:
   :show-inheritance:
```
