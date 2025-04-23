# Transformer

The Transformer class is responsible for connecting a source to a sink where records are
read from the source and written to a sink.

The Transformer supports two modes:
- No streaming
- Streaming

**No streaming**

In this mode, the Transformer reads records from a source and writes to an intermediate graph. One can then use this
intermediate graph as a substrate for various graph operations.


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

**Streaming**

In this mode, records are read from a source and written to sink, on-the-fly.

```python
from kgx.transformer import Transformer

input_args = {'filename': ['graph_nodes.tsv', 'graph_edges.tsv'], 'format': 'tsv'}
output_args = {'filename': 'graph.json', 'format': 'json'}

t = Transformer(stream=True)

# read from TSV and write to JSON
t.transform(input_args=input_args, output_args=output_args)
```

## Inspecting the Knowledge Data Flow

Note that `transform` operation accepts an optional inspect _Callable_ argument which injects node/edge data 
stream inspection into the `Transform.process` operation of `Transform.transform` operations.  See the unit  
test module in the KGX project [tests/integration/test_transform.py](https://github.com/biolink/kgx/blob/master/tests/integration/test_transform.py) for an example of usage of this callable 
argument. 

This feature, when coupled with the `--stream` and a 'null' Transformer Sink  (i.e. `output_args = {'format': 'null'}'`),
allows "just-in-time" processing of the nodes and edges of huge graphs without incurring a large in-memory footprint.

## Provenance of Nodes and Edges

Biolink Model 2.0 specified new [properties for edge provenance](https://github.com/biolink/kgx/blob/master/specification/kgx-format.md#edge-provenance) to replace the (now deprecated) `provided_by` 
provenance property (the `provided_by` property may still be used for node annotation).  

One or more of these provenance properties may optionally be inserted as dictionary entries into the input arguments
to specify default global values for these properties. Such values will be used when an edge lacks an explicit 
provenance property. If one does not specify such a global property, then the algorithm heuristically infers and sets 
a default `knowledge_source` value.

```python
from kgx.transformer import Transformer

input_args = {
    'filename': [
        'graph_nodes.tsv',
        'graph_edges.tsv'],
    'format': 'tsv',
    'provided_by': "My Test Source",
    'aggregator_knowledge_source': "My Test Source"

}

t = Transformer()

# read from TSV 
t.transform(input_args=input_args)

# use the transformed graph
t.store.graph.nodes()
t.store.graph.edges()
```

## InfoRes Identifier Rewriting

The `provided_by` and/or `knowledge_source` _et al._ field values of KGX node and edge records generally contain a name 
of a knowledge source for the node or edge.  In some cases, (e.g. Monarch)  such values in source knowledge sources 
could be quite verbose. To normalize such names to a concise standard, Biolink Model uses
**Information Resource** ("InfoRes") CURIE identifiers.  

To help generate and document such InfoRes identifiers, the provenance property values may optionally trigger a rewrite 
of their knowledge source names to a candidate InfoRes, as follows:

1. Setting the provenance property to a boolean **True* or (case-insensitive) string **"True"** triggers a simple 
reformatting of knowledge source names into lower case alphanumeric strings removing non-alphanumeric characters 
and replacing space delimiting words, with hyphens.

2. Setting the provenance property  to a boolean **False* or (case-insensitive) string **"False"** suppresses the 
given provenance annotation on the output graph.

3. Providing a tuple with a single string argument not equal to **True**, then the string is assumed to be a standard 
regular expression to match against knowledge source names. If you do not provide any other string
argument (see below), then a matching substring in the name triggers deletion of the matched pattern.  The simple 
reformatting (as in 1 above) is then applied to the resulting string.

4. Similar to 2 above, except providing a second string in the tuple which is substituted for the regular expression 
matched string, followed by simple reformatting.

5. Providing a third string in the tuple to add a prefix string to the name (as a separate word) of all the generated 
InfoRes identifiers.  Note that if one sets the first and second elements of the tuple to empty strings, the result
is the simple addition of a prefix to the provenance property value. Again, the algorithm then applies the simple 
reformatting rules, but no other internal changes.

The unit tests provide examples of these various rewrites, in the KGX project
[tests/integration/test_transform.py](https://github.com/biolink/kgx/blob/master/tests/integration/test_transform.py).

The catalog of inferred InfoRes mappings onto knowledge source names is available programmatically, after completion 
of transform call by using the `get_infores_catalog()` method of the **Transformer** class.

## kgx.transformer


```{eval-rst}
.. automodule:: kgx.transformer
   :members:
   :inherited-members:
   :show-inheritance:
```
