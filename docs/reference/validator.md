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

## Biolink Model Versioning

By default, the Validator validates against the latest Biolink Model release supported by the current Biolink Model Toolkit, but this default may be overridden at the Validator class level using the `Validator.set_biolink_model(version="#.#.#")` where  **#.#.#**  is the _major.minor.patch_ semantic versioning of the desired Biolink Model release.  The kgx **validate** CLI operation also has an optional `biolink_release` argument for the same purpose.

## kgx.validator


```eval_rst
.. automodule:: kgx.validator
   :members:
   :inherited-members:
   :show-inheritance:
```
