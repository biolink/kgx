# TRAPI Sink

For TRAPI details, see: [Reasoner API Documentation](https://github.com/NCATSTranslator/ReasonerAPI)

The `TrapiSink` is responsible for writing data in TRAPI (Translator Reasoner API) format. This sink converts KGX nodes
and edges to the TRAPI KnowledgeGraph format.

## Usage

```python
from kgx.sink.trapi_sink import TrapiSink

# Initialize the sink
sink = TrapiSink(
    filename="output.json",
    format="json",
    biolink_version="3.1.0",
    knowledge_source="infores:my-knowledge-provider"
)

# Write nodes and edges
for node_record in node_records:
    sink.write_node(node_record)
    
for edge_record in edge_records:
    sink.write_edge(edge_record)
    
# Finalize and write to file
sink.finalize()
```

## Parameters

- `filename` (str): The filename to write to
- `format` (str, optional): The file format (`json` or `jsonl`), defaults to `json`
- `compression` (str, optional): The compression type (`gz`)
- `biolink_version` (str, optional): The Biolink Model version to use
- `knowledge_source` (str, optional): The default knowledge source to use when not provided in records

## Format Conversion

### Node Mappings
- KGX id → TRAPI node ID (dictionary key)
- KGX name → TRAPI name
- KGX category → TRAPI categories (ensuring biolink: prefix)
- KGX properties → TRAPI attributes with proper attribute_type_id

### Edge Mappings
- KGX subject → TRAPI subject
- KGX predicate → TRAPI predicate
- KGX object → TRAPI object
- KGX primary_knowledge_source → TRAPI sources (with resource_role = "primary_knowledge_source")
- KGX provided_by → TRAPI sources (aggregator_knowledge_source)
- KGX pks_record_urls → TRAPI source_record_urls
- KGX qualifiers → TRAPI qualifiers

## Output Format

The sink can output data in two formats:

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

## Example Output

### Node in TRAPI format
```json
{
  "name": "Haptoglobin",
  "categories": ["biolink:Protein"],
  "attributes": [
    {
      "attribute_type_id": "biolink:description",
      "value": "Haptoglobin protein",
      "value_type_id": "biolink:Phenomenon"
    },
    {
      "attribute_type_id": "biolink:provided_by",
      "value": ["UniProtKB"],
      "value_type_id": "biolink:Agent"
    }
  ]
}
```

### Edge in TRAPI format
```json
{
  "subject": "UniProtKB:P00738",
  "predicate": "biolink:gene_associated_with_condition", 
  "object": "MONDO:0005002",
  "attributes": [
    {
      "attribute_type_id": "biolink:relation",
      "value": "RO:0002326"
    }
  ],
  "sources": [
    {
      "resource_id": "infores:semmeddb",
      "resource_role": "primary_knowledge_source",
      "source_record_urls": ["https://example.org/record/123"]
    }
  ],
  "qualifiers": [
    {
      "qualifier_type_id": "biolink:object_aspect_qualifier",
      "qualifier_value": "activity"
    }
  ]
}
```