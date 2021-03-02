# Source

A Source is responsible for reading data as records from a store where the store is either
a file, local or remote database.

Every source must subclass `kgx.source.source.Source` class and implement the following methods:
- `parse`
- `read_nodes`
- `read_edges`


### kgx.source.source

```eval_rst
.. automodule:: kgx.source.source
   :members:
   :inherited-members:
   :show-inheritance:
```

### kgx.source.graph_source

```eval_rst
.. automodule:: kgx.source.graph_source
   :members:
   :inherited-members:
   :show-inheritance:
```

### kgx.source.tsv_source

```eval_rst
.. automodule:: kgx.source.tsv_source
   :members:
   :inherited-members:
   :show-inheritance:
```

### kgx.source.json_source

```eval_rst
.. automodule:: kgx.source.json_source
   :members:
   :inherited-members:
   :show-inheritance:
```

### kgx.source.jsonl_source

```eval_rst
.. automodule:: kgx.source.jsonl_source
   :members:
   :inherited-members:
   :show-inheritance:
```

### kgx.source.trapi_source

```eval_rst
.. automodule:: kgx.source.trapi_source
   :members:
   :inherited-members:
   :show-inheritance:
```

### kgx.source.obograph_source

```eval_rst
.. automodule:: kgx.source.obograph_source
   :members:
   :inherited-members:
   :show-inheritance:
```

### kgx.source.sssom_source

```eval_rst
.. automodule:: kgx.source.sssom_source
   :members:
   :inherited-members:
   :show-inheritance:
```

### kgx.source.neo_source

```eval_rst
.. automodule:: kgx.source.neo_source
   :members:
   :inherited-members:
   :show-inheritance:
```

### kgx.source.rdf_source

```eval_rst
.. automodule:: kgx.source.rdf_source
   :members:
   :inherited-members:
   :show-inheritance:
```

### kgx.source.owl_source

```eval_rst
.. automodule:: kgx.source.owl_source
   :members:
   :inherited-members:
   :show-inheritance:
```

### kgx.source.sparql_source

```eval_rst
.. automodule:: kgx.source.sparql_source
   :members:
   :inherited-members:
   :show-inheritance:
```