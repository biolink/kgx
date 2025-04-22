# Knowledge Graphs in Memory

KGX makes use of an in-memory labelled property graph for
representing a Knowledge Graph.

To support a wide variety of graph libraries, KGX has a Graph API
which abstracts over the underlying graph store.

Should you want to add support for a new graph store,
- create a new class that extends `kgx.graph.base_graph.BaseGraph`.
- modify the `graph_store` variable in [kgx/config.yml]().


### kgx.graph.base_graph.BaseGraph

`BaseGraph` is the base Graph API that can be used to abstract over any graph, 
as long as the graph is capable of successfully representing a property graph.


```{eval-rst}
.. automodule:: kgx.graph.base_graph
   :members:
   :inherited-members:
   :show-inheritance:
``` 


## kgx.graph.nx_graph.NxGraph

NxGraph is basically an abstraction on top of [networkx.MultiDiGraph](https://networkx.org/documentation/stable/reference/classes/multidigraph.html).

The NxGraph subclasses `kgx.graph.base_graph.BaseGraph` and implements all
the methods defined in `BaseGraph`.


```{eval-rst}
.. automodule:: kgx.graph.nx_graph
   :members:
   :inherited-members:
   :show-inheritance:
``` 
 