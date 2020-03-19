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
HGNC:11603  TBX4    Gene    https://www.genenames.org/data/gene-symbol-report/#!/hgnc_id/HGNC:11603 PMID:12315421|PMID:12342323 property_value1
MONDO:0005002   chronic obstructive pulmonary disease   Disease http://purl.obolibrary.org/obo/MONDO_0005002    PMID:12312312
```

Structure for `edges.tsv`,
```
subject edge_label  object   relation  provided_by  publications
HGNC:11603  contributes_to_condition    MONDO:0005002   RO:0003304  https://archive.monarchinitiative.org/#gwascatalog  PMID:26634245|PMID:26634244
```

If a property has more than one value then it can be represented with a `|` delimiter.


### JSON format

KGX recommends the following JSON structure,
```json
{
    "nodes" : [
      {
        "id": "HGNC:11603",
        "name": "TBX4",
        "category": ["Gene"],
        "iri": "https://www.genenames.org/data/gene-symbol-report/#!/hgnc_id/HGNC:1160",
        "publications": ["PMID:12315421", "PMID:12342323"]
      },
      {
        "id": "MONDO:0005002",
        "name": "chronic obstructive pulmonary disease",
        "category": ["Disease"],
        "iri": "http://purl.obolibrary.org/obo/MONDO_0005002",
        "publications": ["PMID:12312312"]
      }
    ],
    "edges" : [
      {
        "subject": "HGNC:11603",
        "edge_label": "contributes_to_condition",
        "object": "MONDO:0005002",
        "relation": "RO:0003304",
        "provided_by": "https://archive.monarchinitiative.org/#gwascatalog",
        "publications": ["PMID:26634245", "PMID:26634244"]
      }
    ]
}
```

If a property has more than one value then it can be represented as a list of values.
