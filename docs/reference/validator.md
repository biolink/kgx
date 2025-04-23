# KGX Validator

The KGX Validator is a tool that validates graph data for compliance with the 
[Biolink Model](https://biolink.github.io/biolink-model/). It performs validation across multiple facets of the 
knowledge graph, helping ensure data adheres to established standards.

## Key Validation Features

The validator checks the following aspects of your knowledge graph:

### Node Validation
- **Required Properties**: Verifies that all required node properties (as defined by Biolink Model) are present
- **Property Types**: Ensures node properties have the expected value types (string, URI/CURIE, double, list)
- **Property Values**: Validates that node identifiers are properly formed CURIEs with prefixes defined in the Biolink Model JSON-LD context
- **Categories**: Validates that node categories exist in the Biolink Model, are properly formatted in CamelCase

### Edge Validation
- **Required Properties**: Verifies that all required edge properties are present
- **Property Types**: Ensures edge properties have the expected value types
- **Property Values**: Validates that subject and object properties are properly formed CURIEs with prefixes defined in the Biolink Model
- **Predicates**: Validates that edge predicates exist in the Biolink Model and follow proper snake_case formatting

### Biolink Model Integration
The validator leverages the [Biolink Model Toolkit (BMT)](https://github.com/biolink/biolink-model-toolkit) to:
- Access Biolink Model elements (classes, slots, predicates)
- Determine required properties for nodes and edges
- Verify node categories and edge predicates against the model
- Validate property value types based on model specifications
- Support validation against specific versions of the Biolink Model

## Error Tracking
The validator captures and reports the following types of errors:
- Missing required properties
- Invalid property types
- Invalid property values
- Invalid categories and predicates
- Missing or malformed CURIEs

## Usage Examples

### Basic Validation
```python
from kgx.validator import Validator
from kgx.transformer import Transformer

# Load a graph
transformer = Transformer()
transformer.transform('input.json')
graph = transformer.graph

# Validate the graph
validator = Validator()
validator.validate(graph)

# Get validation errors
errors = validator.get_errors()
```

### Using a Specific Biolink Model Version
```python
from kgx.validator import Validator

# Set Biolink Model version for validation
Validator.set_biolink_model("3.1.0")

# Create validator with the specified version
validator = Validator()
validator.validate(graph)
```

### Streaming Validation for Large Graphs
For very large graphs, use the streaming mode to minimize memory usage:

```bash
kgx validate --input-format tsv --stream=True input_nodes.tsv input_edges.tsv
```

## Command Line Interface
KGX provides a command-line interface for validation:

```bash
kgx validate --input-format tsv \
             --biolink-release 3.1.0 \
             --error-log validation_errors.json \
             input_nodes.tsv input_edges.tsv
```

## Understanding Validation Messages

Validation errors are categorized into different levels:
- **INFO**: Informational messages about suggested improvements
- **WARNING**: "Should" level recommendations for better compliance
- **ERROR**: "Must" level requirements that violate model specifications

Example validation error output:
```json
{
  "ERROR": {
    "INVALID_CATEGORY": {
      "Category 'gene' is not in CamelCase form": ["NCBIGene:1234"]
    },
    "MISSING_NODE_PROPERTY": {
      "Required node property 'category' is missing": ["MONDO:0005737"]
    }
  }
}
```

## Benefits of Validation
- Ensures consistency across knowledge graphs
- Identifies data quality issues early
- Facilitates data integration from multiple sources
- Improves interoperability between systems
- Provides guidance on Biolink Model compliance

By using the KGX Validator, you can ensure your knowledge graphs adhere to the Biolink Model specifications, 
making them more interoperable and useful for downstream applications.