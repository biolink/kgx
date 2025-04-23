# Meta Knowledge Graph

The Meta Knowledge Graph operation takes an instance of `kgx.graph.base_graph.BaseGraph` and
generates Translator API (TRAPI) Release 1.1 compatible knowledge map for the entire graph.

This operation generates graph summary as a JSON (or YAML) in a format that is compatible with the content metadata 
standards of the[Knowledge Graph Exchange (KGE) Archive](https://github.com/NCATSTranslator/Knowledge_Graph_Exchange_Registry).

The main entry point is the `kgx.graph_operations.meta_knowledge_graph.generate_meta_knowledge_graph` method.

The tool does detect and logs anomalies in the graph (defaults reporting  to stderr, but may be reset to a file 
using the `error_log` parameter)

**Note:** To generate a summary statistics YAML that is compatible with Knowledge Graph Hub dashboard,
refer to [Summarize Graph operation](summarize_graph.md).

## Streaming Data Processing Mode

For very large graphs, the Meta Knowledge Graph operation now successfully processes graph data using data 
streaming (command flag `--stream=True`) which significantly minimizes the memory footprint required to process 
such graphs.

## Provenance Statistics

The Meta Knowledge Graph operation can count numbers of nodes and edges by Biolink 2.0 
`biolink:knowledge_source` provenance (and related `is_a` descendant slot terms). 
The `node_facet_properties` and `edge_facet_properties` CLI (and code method) arguments need to be 
explicitly set to specify which provenance slot names are to be counted in a given graph (by default,
`provided_by` slots used for nodes and `knowledge_source` slots used for edges).

## kgx.graph_operations.meta_knowledge_graph

```{eval-rst}
.. automodule:: kgx.graph_operations.meta_knowledge_graph
   :members:
   :inherited-members:
   :show-inheritance:
```