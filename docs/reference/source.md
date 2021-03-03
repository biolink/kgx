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

```eval_rst
.. automodule:: kgx.source.source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.graph_source

```eval_rst
.. automodule:: kgx.source.graph_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.tsv_source

```eval_rst
.. automodule:: kgx.source.tsv_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.json_source

```eval_rst
.. automodule:: kgx.source.json_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.jsonl_source

```eval_rst
.. automodule:: kgx.source.jsonl_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.trapi_source

```eval_rst
.. automodule:: kgx.source.trapi_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.obograph_source

```eval_rst
.. automodule:: kgx.source.obograph_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.sssom_source

```eval_rst
.. automodule:: kgx.source.sssom_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.neo_source

```eval_rst
.. automodule:: kgx.source.neo_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.rdf_source

```eval_rst
.. automodule:: kgx.source.rdf_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.owl_source

```eval_rst
.. automodule:: kgx.source.owl_source
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.source.sparql_source

```eval_rst
.. automodule:: kgx.source.sparql_source
   :members:
   :inherited-members:
   :show-inheritance:
```