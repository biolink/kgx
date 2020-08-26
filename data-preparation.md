# Data preparation for use with KGX

## Preparing data files

KGX support various file formats but there are certain assumptions made while 
working with these files. 

These are especially true for TSV/CSV and JSON format for things like, 
- How information about nodes are represented
- How information about edges are represented


### TSV/CSV format

When your data source is a TSV/CSV, ensure that there are two separate files: one for 
nodes and another for edges. Example, `nodes.tsv` and `edges.tsv`.

Structure for `nodes.tsv`,
```
id  name    category    iri publications
HGNC:11603  TBX4    biolink:Gene    https://www.genenames.org/data/gene-symbol-report/#!/hgnc_id/HGNC:11603 PMID:12315421|PMID:12342323 property_value1
MONDO:0005002   chronic obstructive pulmonary disease   biolink:Disease http://purl.obolibrary.org/obo/MONDO_0005002    PMID:12312312
```

Node properties,

- `id`: must be a CURIE (required)
- `name`: name of the entity; definition of a concept (optional)
- `category`: must be a [Biolink Model class](https://biolink.github.io/biolink-model/docs/Classes.html) (required)

Any additional columns, such as `iri` and `publications` in the example above, will end up as node properties. You can have as many properties as you want. But we recommend reusing existing Biolink Model fields first.

Structure for `edges.tsv`,
```
subject edge_label  object   relation  provided_by  publications
HGNC:11603  biolink:contributes_to    MONDO:0005002   RO:0003304  https://archive.monarchinitiative.org/#gwascatalog  PMID:26634245|PMID:26634244
```

Edge properties,

- `subject`: must be a CURIE (required)
- `edge_label`: must be a [Biolink Model type](https://biolink.github.io/biolink-model/docs/related_to) (required)
- `object`: must be a CURIE (required)
- `relation`: must be a CURIE (required); this field can be a more specific ontology term that well characterizes this relationship

Any additional columns, such as `provided_by` and `publications` in the example above, will end up as edge properties. You can have as many properties as you want. But we recommend reusing existing Biolink Model fields first.

If a property has more than one value then it can be represented with a `|` delimiter.



### JSON format

KGX recommends the following JSON structure,
```json
{
    "nodes" : [
      {
        "id": "HGNC:11603",
        "name": "TBX4",
        "category": ["biolink:Gene"],
        "iri": "https://www.genenames.org/data/gene-symbol-report/#!/hgnc_id/HGNC:1160",
        "publications": ["PMID:12315421", "PMID:12342323"]
      },
      {
        "id": "MONDO:0005002",
        "name": "chronic obstructive pulmonary disease",
        "category": ["biolink:Disease"],
        "iri": "http://purl.obolibrary.org/obo/MONDO_0005002",
        "publications": ["PMID:12312312"]
      }
    ],
    "edges" : [
      {
        "subject": "HGNC:11603",
        "edge_label": "biolink:contributes_to",
        "object": "MONDO:0005002",
        "relation": "RO:0003304",
        "provided_by": "https://archive.monarchinitiative.org/#gwascatalog",
        "publications": ["PMID:26634245", "PMID:26634244"]
      }
    ]
}
```

If a property has more than one value then it can be represented as a list of values.
