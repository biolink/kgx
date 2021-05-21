# Meta Knowledge Graph

The Meta Knowledge Graph operation takes an instance of `kgx.graph.base_graph.BaseGraph` and
generates Translator API (TRAPI) Release 1.1 compatible knowledge map for the entire graph.

This operation generates graph summary as a JSON (or YAML) in a format that is compatible with the content metadata standards of the 
[Knowledge Graph Exchange (KGE) Archive](https://github.com/NCATSTranslator/Knowledge_Graph_Exchange_Registry).

The main entry point is the `kgx.graph_operations.meta_knowledge_graph.generate_meta_knowledge_graph` method.

**Note:** To generate a summary statistics YAML that is compatible with Knowledge Graph Hub dashboard,
refer to [Summarize Graph operation](summarize_graph.md).

## Streaming Data Processing Mode

For very large graphs, the Meta Knowledge Graph operation may now successfully process graph data equally well  using data streaming (command flag `--stream=True`) which significantly minimizes the memory footprint required to process such graphs.

## kgx.graph_operations.meta_knowledge_graph

```eval_rst
.. automodule:: kgx.graph_operations.meta_knowledge_graph
   :members:
   :inherited-members:
   :show-inheritance:
```