# Prefix Manager

In KGX, the `PrefixManager` acts as a central resource for,
- CURIE to IRI expansion
- IRI to CURIE contraction 

Under the hood, `PrefixManager` makes use of [prefixcommons-py](https://github.com/prefixcommons/prefixcommons-py).

Each time the `PrefixManager` class is initialized, it makes use of the Biolink Model
JSON-LD context for a default set of prefix to IRI mappings. 

These defaults can be overridden by using `update_prefix_map` and providing your custom
mappings.


## kgx.prefix_manager


```{eval-rst}
.. automodule:: kgx.prefix_manager
   :members:
   :inherited-members:
   :show-inheritance:
```