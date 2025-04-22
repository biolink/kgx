# Clique Merge


The Clique Merge operation performs a series of operations on your target (input) graph:
- Build cliques from nodes in the target graph
- Elect a leader for each individual clique
- Move all edges in a clique to the leader node
 
The main entry point is `kgx.graph_operations.clique_merge.clique_merge` method which
takes an instance of `kgx.graph.base_graph.BaseGraph`.


**Build cliques from nodes in the target graph**

Given a target graph, create a clique graph where nodes in the same clique are connected via
`biolink:same_as` edges.

In the target graph, you can define nodes that belong to the same clique as follows:
- Having `biolink:same_as` edges between nodes (preferred and consistent with Biolink Model)
- Having `same_as` node property on a node that lists all equivalent nodes (deprecated)


**Elect a leader for each individual clique**

Once the clique graph is built, go through each clique and elect a representative node or
leader node for that clique. 

Elect leader for each clique based on three election criteria, listed in the order 
in which they are checked:
- **Leader annotation:** Elect the leader node for a clique based on `clique_leader` 
    annotation on the node
- **Prefix prioritization:** Elect the leader node for a clique that has a prefix which is 
    of the highest priority in the identifier prefixes list, as defined in the Biolink Model
- **Prefix prioritization fallback:** Elect the leader node for a clique that has a prefix 
    which is the first in an alphabetically sorted list of all ID prefixes within the clique



**Move all edges in a clique to the leader node**

The last step is edge consolidation where all the edges from nodes in a clique are moved
to the leader node. 

The original subject and object node of an edge is tracked via the  `_original_subject` and 
`_original_object` edge property.


## kgx.graph_operations.clique_merge

```{eval-rst}
.. automodule:: kgx.graph_operations.clique_merge
   :members:
   :inherited-members:
   :show-inheritance:
```
