# Summarize Graph

The Summarize Graph operation takes an instance of `kgx.graph.base_graph.BaseGraph` and
generates summary statistics for the entire graph.

This operation generates summary as a YAML (or JSON) in a format that is compatible with
the [Knowledge Graph Hub dashboard](https://knowledge-graph-hub.github.io/kg-covid-19-dashboard/).

The main entry point is the `kgx.graph_operations.summarize_graph.generate_graph_stats` method.

The tool does detect and logs anomalies in the graph (defaults reporting  to stderr, but may be reset to a file using 
the `error_log` parameter)

**Note:** To generate a summary statistics YAML that is consistent with Translator API (TRAPI) Release 1.1
standards, refer to [Meta Knowledge Graph](meta_knowledge_graph.md).


## Streaming Data Processing Mode

For very large graphs, the Graph Summary operation may now successfully process graph data equally well  using data 
streaming (command flag `--stream=True`) which significantly minimizes the memory footprint required to process such graphs.

## kgx.graph_operations.summarize_graph

```{eval-rst}
.. automodule:: kgx.graph_operations.summarize_graph
   :members:
   :inherited-members:
   :show-inheritance:
```