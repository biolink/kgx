# Validator

The Validator validates an instance of kgx.graph.base_graph.BaseGraph for Biolink Model compliance.

To validate a graph,

```python
from kgx.validator import Validator
v = Validator()
v.validate(graph)
```

## Streaming Data Processing Mode

For very large graphs, the Validator operation may now successfully process graph data equally well using data streaming (command flag `--stream=True`) which significantly minimizes the memory footprint required to process such graphs.

## kgx.validator


```eval_rst
.. automodule:: kgx.validator
   :members:
   :inherited-members:
   :show-inheritance:
```
