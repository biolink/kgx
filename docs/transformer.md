# Transformer

The Transformer class is responsible for connecting a source to a sink where records are
read from the source and written to a sink.

The Transformer supports two modes:
- No streaming
- Streaming

## No streaming

In this mode, records are read from a source and written to an intermediate graph. This
intermediate graph can then be used as a substrate for various graph operations.


```python
from kgx.transformer import Transformer

input_args = {'filename': ['graph_nodes.tsv', 'graph_edges.tsv'], 'format': 'tsv'}
output_args = {'filename': 'graph.json', 'format': 'json'}

t = Transformer(stream=False)

# read from TSV
t.transform(input_args=input_args)

# The intermediate graph store can be accessed via t.store.graph

# write to JSON
t.save(output_args=output_args)
```

## Streaming

In this mode, records are read from a source and written to sink, on-the-fly.

``` note:: Using streaming disables certain functionalities like the ability to apply graph operations
```

```python
from kgx.transformer import Transformer

input_args = {'filename': ['graph_nodes.tsv', 'graph_edges.tsv'], 'format': 'tsv'}
output_args = {'filename': 'graph.json', 'format': 'json'}

t = Transformer(stream=True)

# read from TSV and write to JSON
t.transform(input_args=input_args, output_args=output_args)
```

---

#### kgx.transformer

```eval_rst
.. automodule:: kgx.transformer
   :members:
   :inherited-members:
   :show-inheritance:
```
