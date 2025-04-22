# TRAPI Source

For TRAPI details, see: [Reasoner API Documentation](https://github.com/NCATSTranslator/ReasonerAPI)

The `TrapiSource` is responsible for reading data from TRAPI (Translator Reasoner API) JSON format and converting 
it into KGX format.

## Usage

```python
from kgx.source.trapi_source import TrapiSource
from kgx.graph.nx_graph import NxGraph

# Create a graph
graph = NxGraph()

# Initialize the source
source = TrapiSource(graph)

# Parse TRAPI file
for record in source.parse(
    filename="input.json",
    format="json"
):
    # Process records (nodes and edges)
    if record:
        if len(record) == 4:  # Edge
            # Handle edge
            pass
        else:  # Node
            # Handle node
            pass
```

## Parameters

- `filename` (str): The filename to read from
- `format` (str, optional): The file format (`json` or `jsonl`), defaults to `json`
- `compression` (str, optional): The compression type (`gz`)
- `provided_by` (str, optional): The name of the provider to assign to records

## Format Conversion

### Node Mappings
- TRAPI node ID → KGX id
- TRAPI name → KGX name
- TRAPI categories → KGX category
- TRAPI attributes → KGX properties

### Edge Mappings
- TRAPI subject → KGX subject
- TRAPI predicate → KGX predicate
- TRAPI object → KGX object
- TRAPI sources → KGX primary_knowledge_source and provided_by
- TRAPI source_record_urls → KGX pks_record_urls
- TRAPI qualifiers → KGX qualifiers

## Input Format

The source can read data from two formats:

1. **JSON format** (default) - A single JSON file with the structure:
   ```json
   {
     "knowledge_graph": {
       "nodes": {
         "CURIE1": { ... },
         "CURIE2": { ... }
       },
       "edges": {
         "EDGE1": { ... },
         "EDGE2": { ... }
       }
     }
   }
   ```

2. **JSONLines format** - Each line contains a separate JSON object:
   - First line: Header with metadata
   - Subsequent lines: Individual nodes and edges

## Example Input

### Node in TRAPI format
```json
{
  "CURIE1": {
    "name": "Haptoglobin",
    "categories": ["biolink:Protein"],
    "attributes": [
      {
        "attribute_type_id": "biolink:description",
        "value": "Haptoglobin protein",
        "value_type_id": "biolink:Phenomenon"
      }
    ]
  }
}
```

### Node in KGX format (after conversion)
```json
{
  "id": "CURIE1",
  "name": "Haptoglobin",
  "category": ["biolink:Protein"],
  "description": "Haptoglobin protein"
}
```

### Edge in TRAPI format
```json
{
  "edge1": {
    "subject": "UniProtKB:P00738",
    "predicate": "biolink:gene_associated_with_condition", 
    "object": "MONDO:0005002",
    "sources": [
      {
        "resource_id": "infores:semmeddb",
        "resource_role": "primary_knowledge_source"
      }
    ]
  }
}
```

### Edge in KGX format (after conversion)
```json
{
  "subject": "UniProtKB:P00738",
  "predicate": "biolink:gene_associated_with_condition", 
  "object": "MONDO:0005002",
  "primary_knowledge_source": "infores:semmeddb"
}
```