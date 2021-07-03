# Meta Knowledge Graph

The Meta Knowledge Graph operation takes an instance of `kgx.graph.base_graph.BaseGraph` and
generates Translator API (TRAPI) Release 1.1 compatible knowledge map for the entire graph.

This operation generates graph summary as a JSON (or YAML) in a format that is compatible with the content metadata standards of the 
[Knowledge Graph Exchange (KGE) Archive](https://github.com/NCATSTranslator/Knowledge_Graph_Exchange_Registry).

The main entry point is the `kgx.graph_operations.meta_knowledge_graph.generate_meta_knowledge_graph` method.

The tool does detect and logs anomalies in the graph (defaults reporting  to stderr, but may be reset to a file using the `error_log` parameter)

**Note:** To generate a summary statistics YAML that is compatible with Knowledge Graph Hub dashboard,
refer to [Summarize Graph operation](summarize_graph.md).

## Streaming Data Processing Mode

For very large graphs, the Meta Knowledge Graph operation now successfully processes graph data using data streaming (command flag `--stream=True`) which significantly minimizes the memory footprint required to process such graphs.

## Provenance Statistics

The Meta Knowledge Graph operation can count numbers of nodes and edges by Biolink 2.0 `biolink:knowledge_source` provenance (and related `is_a` descendant slot terms). The `node_facet_properties` and `edge_facet_properties` CLI (and code method) arguments need to be explicitly used to specify which provenance slot names are to be counted in a given graph (by default, `provided_by` slots are used for nodes and `knowledge_source` slots used for edges).

## InfoRes Identifier Rewriting

The `provided_by` field value of KGX node and edge records generally contains the name of a knowledge source for the node or edge. Previously, such values could be verbose descriptions. The latest Biolink Model standards (2.0) is moving towards "Information Resource" ("InfoRes") CURIE identifiers.  As a partial help to generating and documenting such InfoRes identifiers, the meta-knowledge-graph graph summary takes an optional `--infores-rewrite` which is a list of strings which satisfy several use cases of knowledge source name to InfoRes remapping, as follows:

1. `--infores-rewrite true`: is a simple boolean specification that triggers a simple reformatting of knowledge source names into lower case alphanumeric strings removing non-alphanumeric characters and replacing space delimiting words, with hyphens.

1. `--infores-rewrite <regex>`: if given as a single argument, a provided string argument `<regex>` not equal to 'true', then it is taken as a standard (Pythonic) regular expression to match against knowledge source names. If no other string argument is provided (see below), then a matching substring in the name is simply deleted from the name, prior to a standard rewrite (as in 1 above) is applied.

1. `--infores-rewrite <regex>,<substr>`: similar to 2 above, except that if a second `<substr>` string is provided, then all `<regex>` are replaced with the `<substr>`

1. `--infores-rewrite <regex>,<substr>,<prefix>`: similar to 3 above, except that if a third `<prefix>` string is provided, then then `<prefix>` is added to the front of all generated InfoRes identifiers (as a separate word).  Note that if  `<regex>` and `<substr>` are set to empty strings and `<prefix>` non-empty (That is, `--infores-rewrite ",,<prefix>"`), the result is the simple addition of a prefix to the name, followed by the standard reformatting of the name (as in use case 1 above), but no other internal changes.

Although not (yet) directly exposed in the KGX CLI, the catalog of inferred InfoRes mappings onto knowledge source names is available programmatically, after completion of meta-knowledge-graph graph summary parsing of the input knowledge graph, using the `get_infores_catalog()` method of the `MetaKnowledgeGraph` class.

## kgx.graph_operations.meta_knowledge_graph

```eval_rst
.. automodule:: kgx.graph_operations.meta_knowledge_graph
   :members:
   :inherited-members:
   :show-inheritance:
```