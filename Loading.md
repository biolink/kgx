# Loading CSV into a clean Neo4j database

To perform a bulk import of nodes and edges into a clean Neo4j database, make use of `neo4j-admin`.

Loading via `neo4j-admin import` is very fast and performant.

Before loading, ensure that `nodes.csv` and `edges.csv` have proper headers.


### Prepare nodes CSV

Make sure that each column in `nodes.csv` has an associated header that defines how that column should be interpreted by the loader.

For example,
```
:ID,name:string,category:LABEL
x:1,x1,named_thing
x:2,x2,named_thing
```

- `:ID` - tells the loader that the column is supposed to be treated as node `id` property. (required)
- `name:string` - tells the loader that the column is supposed to be treated as node `name` property. `:string` tells the loader to treat the column value as a string type. If no type is provided then the value is treated as a string, by default.
- `category:LABEL` - tells the loader that the column is supposed to be treated as node `category` property. Additionally, `:LABEL` tells the loader to treat this column as the node label. (required)

Similary, any additional columns can be defined with the headers.


### Prepare edges CSV

Make sure that each column in `edges.csv` has an associated header that defines how that column should be interpreted by the loader.

For example,
```
:START_ID,:TYPE,:END_ID,relation,provided_by
x:1,part_of,x:2,part_of,test
```

- `:START` - tells the loader that the column refers to the start node of an edge. The value should correspond to a node `id`. (required)
- `:TYPE` - tells the loader to treat this column as the relationship type of an edge. (required)
- `:END` - tells the loader that the column refers to the end node of an edge. The value should correspond to a node `id`. (required)
- `relation` - tells the loader to treat this column as `relation` property of an edge.
- `provided_by` - tells the loader to treat this column as `provided_by` property of an edge.

Similary, any additional columns can be defined with the headers.


### Bulk import

Once both `nodes.csv` and `edges.csv` are prepared with their proper headers, run the `neo4j-admin import`:
```
neo4j-admin import \
--database=knowledge-graph.db \
--id-type=string \
--array-delimiter=";" \
--nodes nodes.csv \
--relationships edges.csv
```
