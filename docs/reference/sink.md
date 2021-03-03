# Sink

A Sink can be implemented for any file, local, and/or remote store to which a graph can be written to. A Sink is responsible for writing nodes and edges from a graph.

A Sink must subclass `kgx.sink.sink.Sink` class and must implement the following methods:
- `__init__`
- `write_nodes`
- `write_edges`
- `finalize`


#### `__init__` method

The `__init__` method is used to instantiate a Sink with configurations required for writing to a store.
- In the case of files, the `__init__` method will take the `filename` and `format` as arguments
- In the case of a graph store like Neo4j, the `__init__` method will take the `uri`, `username`, and `password` as arguments.

The `__init__` method also has an optional `kwargs` argument which can be used to supply variable number of arguments to this method, depending on the requirements for the store for which the Sink is being implemented.


### `write_nodes` method

- Responsible for receiving a node record and writing to a file/store


### `write_edges` method

- Responsible for receiving an edge record and writing to a file/store


### `finalize` method

Any operation that needs to be performed after writing all the nodes and edges to a file/store must be defined in this method.

For example,
- `kgx.source.tsv_source.TsvSource` has a `finalize` method that closes the file handles and creates an archive, if compression is desired
- `kgx.source.neo_sink.NeoSink` has a `finalize` method that writes any cached node and edge records


## kgx.sink.sink

```eval_rst
.. automodule:: kgx.sink.sink
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.sink.graph_sink

```eval_rst
.. automodule:: kgx.sink.graph_sink
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.sink.tsv_sink

```eval_rst
.. automodule:: kgx.sink.tsv_sink
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.sink.json_sink

```eval_rst
.. automodule:: kgx.sink.json_sink
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.sink.jsonl_sink

```eval_rst
.. automodule:: kgx.sink.jsonl_sink
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.sink.trapi_sink

```eval_rst
.. automodule:: kgx.sink.trapi_sink
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.sink.neo_sink

```eval_rst
.. automodule:: kgx.sink.neo_sink
   :members:
   :inherited-members:
   :show-inheritance:
```

## kgx.sink.rdf_sink

```eval_rst
.. automodule:: kgx.sink.rdf_sink
   :members:
   :inherited-members:
   :show-inheritance:
```
