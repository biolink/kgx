# CURIE Lookup Service

The CURIE Lookup Service supports the ability to lookup labels for a given CURIE.

It does so by pre-loading all the relevant ontologies when the `CurieLookupService`
class is initialized, where only the terms and their `rdfs:label` are loaded into a separate
graph specifically for the purpose of lookup.

The required ontologies are defined in the KGX `config.yml`.


## kgx.curie_lookup_service


```{eval-rst}
.. automodule:: kgx.curie_lookup_service
   :members:
   :inherited-members:
   :show-inheritance:
```
