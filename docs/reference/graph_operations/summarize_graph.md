# Summarize Graph

The Summarize Graph operation takes an instance of `kgx.graph.base_graph.BaseGraph` and
generates summary statistics for the entire graph.

This operation generates summary as a YAML in a format that is compatible with
the [Knowledge Graph Hub dashboard](https://knowledge-graph-hub.github.io/kg-covid-19-dashboard/).

The main entry point is the `kgx.graph_operations.summarize_graph.generate_graph_stats` method.


**Note:** To generate a summary statistics YAML that is consistent with Translator API (TRAPI) 
standards, refer to [Knowledge Map operation](knowledge_map.md).


## kgx.graph_operations.summarize_graph

```eval_rst
.. automodule:: kgx.graph_operations.summarize_graph
   :members:
   :inherited-members:
   :show-inheritance:
```