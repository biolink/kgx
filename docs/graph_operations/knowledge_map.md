# Knowledge Map

The Knowledge Map operation takes an instance of `kgx.graph.base_graph.BaseGraph` and
generates Translator API (TRAPI) compatible knowledge map for the entire graph.

This operation generates graph summary as a YAML in a format that is compatible with
[Knowledge Graph Exchange Registry](https://github.com/NCATSTranslator/Knowledge_Graph_Exchange_Registry).

The main entry point is the `kgx.graph_operations.knowledge_map.generate_knowledge_map` method.


**Note:** To generate a summary statistics YAML that is compatible with Knowledge Graph Hub dashboard,
refer to [Summarize Graph operation](summarize_graph.md).


---

#### kgx.graph_operations.knowledge_map

```eval_rst
.. automodule:: kgx.graph_operations.knowledge_map
   :members:
   :inherited-members:
   :show-inheritance:
```