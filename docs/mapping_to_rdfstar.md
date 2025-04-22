# Mapping kgx to rdf-star

The mapping is specified as mapping between (1) two tables (nodes and edges), two sets of
mappings (column and prefix) and (2) an RDF* graph.

 * RDF*: https://w3c.github.io/rdf-star/cg-spec/

## Input Tables

 * nodes
 * edges

Each table consists of zero or more rows, with a fixed set of columns _C1_.._Cn_,
which each row has zero or more atomic values per column.

The tables are typically serialized as a TSV. When a TSV serialization
is used, multivalued fields are separated by `|`s. Actual `|` values
should be escaped with backquotes.

## Input Mappings

### Column Mappings

A set of column mappings is provided as input.

A mapping `columnToProperty` maps between a column in either node or
edges file and a URI.

If the column header is a CURIE (for example, biolink:name) then CURIE
expansion rules are applied.

A default prefix MAY be assumed, such that for example a column name
`name` is mapped to biolink:name, to which standard CURIE expansion 

Arbitrary mappings can also be provided.

Informative note:

It is conventional to use certain standardized properties from
biolink, and some parts of the kgx tool chain may make assumptions
here, for example, use of `name` (rdfs:label) or `category` (a
subproperty of rdf:type). However, the mapping from kgx to RDF* is
independent of biolink, and can be used with any vocabulary.

### Prefix Mappings (CURIE expansion)

A set of prefix mappings is provided as input.

These may be specified using JSON-LD contexts, via a simple YAML
file. As far as the specification goes, this is a simple pairwise
mapping of

    prefix -> baseURI

The function `uri(CURIE)` maps a CURIE to a URI by splitting the CURIE
on `:` into a PREFIX and LOCAL part. The PREFIX is substituted by
baseURI and appended onto LOCAL to make the URI.

## Node Sets

Each row is uniquely identified by the `id` column. There MUST NOT be
more than one row with the same `id`. Each row MUST have an id specified.

For each row-column combination `(R,C)` for columns other than `id`, a
single triple is generated.

```turtle
uri(R.id) columnToProperty(C) literalValue(R.C) .
```

## Edge Sets

Each edge is optionally uniquely identified by the `id` column. There
MUST NOT be more than one row with same id (TODO).

A triple is emitted for each row, where the triple is quoted and
additional triples are emitted with the core triple as subject for
each column C, where C is not in `{subject,predicate,object}`

```turtle
<< uri(R.subject) uri(R.predicate) uri(R.object) >>
   columnToProperty(C1) literalOrNode(R.C1, C1) ;
   columnToProperty(C2) literalOrNode(R.C2, C2) ;
   ... ;
   columnToProperty(Cn) literalOrNode(R.Cn, Cn) .
```

TODO: id field
