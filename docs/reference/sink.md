# Sink

A Sink is responsible for writing data as records to a store where the store is either
a file, local or remote database.

Every sink must subclass `kgx.sink.sink.Sink` and implement the following methods:
- `write_node`
- `write_edge`
- `finalize`


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
