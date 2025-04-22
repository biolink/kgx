# KGX Schema

This document describes the schema transformation process used to create the KGX schema based on the Biolink Model. The KGX schema defines the structure for knowledge graphs in the KGX format while leveraging the rich semantics of the Biolink Model.

## Overview

### Purpose

KGX (Knowledge Graph Exchange) is designed to be compatible with the Biolink Model while providing a simplified and more practical data model for knowledge graph exchange. The KGX schema extends the Biolink Model by:

1. Adding high-level `Node` and `Edge` classes as parent classes
2. Providing a `KnowledgeGraph` container class
3. Ensuring all Biolink classes and properties are available

The relationship between KGX and Biolink can be summarized as:
- All Biolink classes are available in KGX
- `Node` becomes a parent class of Biolink's `named thing` class
- `Edge` becomes a parent class of Biolink's `association` class
- `KnowledgeGraph` is a KGX-specific class for the container structure

### Benefits of This Approach

This design provides several advantages:

1. **Simplified Interface**: Applications can interact with knowledge graphs using the higher-level Node/Edge classes without needing to understand all Biolink class details.

2. **Full Biolink Compatibility**: All Biolink semantics remain available, allowing detailed typing when needed.

3. **Automated Updates**: When the Biolink Model is updated, the KGX schema can be automatically regenerated to stay in sync.

4. **Container Structure**: The KnowledgeGraph class provides a standard way to package nodes and edges together.

## Technical Implementation

### Schema Generation Process

The KGX schema generation follows these steps:

1. **Base KGX Schema**: Define a base KGX schema (`kgx.yaml`) that imports the Biolink Model and adds KGX-specific classes (Node, Edge, KnowledgeGraph).

2. **Schema Materialization**: Merge all imported schemas (including Biolink) into a single file.

3. **Transformation Specification**: Define a LinkML transformation specification to modify the inheritance hierarchy.

4. **Final Schema Generation**: Apply the transformation to produce the final KGX schema.

### Directory Structure

Key files and directories:

```
kgx/
├── schema/
│   ├── kgx.yaml                       # Base KGX schema that imports Biolink
│   ├── kgx_merged.yaml                # Materialized schema with all imports
│   ├── kgx_complete.yaml              # Final schema with inheritance structure
│   ├── transformations/
│   │   ├── kgx_inheritance.transform.yaml  # Transformation specification
│   └── scripts/
│       ├── generate_transform.py      # Script to generate transformation specs
```

### Makefile Targets

The schema generation process is automated through Makefile targets:

```makefile
# Clean up intermediate schema files
schema-clean:
	@echo "Cleaning up intermediate schema files..."
	rm -f kgx/schema/kgx_merged.yaml kgx/schema/kgx_final.yaml kgx/schema/derived_kgx_schema.yaml
	rm -f kgx/schema/transformations/full_transform.transform.yaml

# Merge imported schemas (biolink-model) into a single file
schema-merge: schema-clean
	@echo "Merging schemas..."
	poetry run gen-linkml --mergeimports --format yaml kgx/schema/kgx.yaml -o kgx/schema/kgx_merged.yaml

# Generate transformation specification
schema-transform: schema-merge
	@echo "Generating transformation specification..."
	poetry run python kgx/schema/scripts/generate_transform.py kgx/schema/kgx_merged.yaml kgx/schema/transformations/full_transform.transform.yaml

# Apply transformation to create final schema
schema-apply: schema-transform
	@echo "Applying transformation..."
	poetry run linkml-map derive-schema -T kgx/schema/transformations/full_transform.transform.yaml -o kgx/schema/kgx_complete.yaml kgx/schema/kgx_merged.yaml

# Complete schema build process
schema: schema-apply
	@echo "Schema build complete. Final schema is at kgx/schema/kgx_complete.yaml"
```

### Regenerating the Schema

To regenerate the schema, simply run:

```bash
make schema
```

This will:
1. Clean up any intermediate files
2. Merge the Biolink schema with KGX-specific classes
3. Generate a transformation specification
4. Apply the transformation to create the final schema

## Transformation Details

### Base Schema (kgx.yaml)

The base schema imports the Biolink Model and defines KGX-specific classes:

```yaml
imports:
  - linkml:types
  - https://w3id.org/biolink/biolink-model

classes:
  KnowledgeGraph:
    description: A knowledge graph represented in KGX format
    slots:
      - nodes
      - edges
  
  Node:
    description: A node in a KGX graph, superclass for NamedThing
    slots:
      - id
      - name
      - description
      - category
      - xref
      - provided by
    
  Edge:
    description: An edge in a KGX graph, superclass for Association
    slots:
      - id
      - subject
      - predicate
      - object
      - relation
      - category
      - provided by
      - knowledge source
      # ... other edge slots ...

slots:
  nodes:
    range: Node
    multivalued: true
    inlined: true
    
  edges:
    range: Edge
    multivalued: true
    inlined: true
```

### Transformation Specification

The transformation specification modifies the inheritance structure:

```yaml
class_derivations:
  # Make "named thing" a child of Node
  "named thing":
    populated_from: "named thing"
    overrides:
      is_a: Node
      
  # Make "association" a child of Edge
  association:
    populated_from: association
    overrides:
      is_a: Edge
```

## Future Improvements

Potential future improvements to the schema generation process:

1. **Automatic Class Case Conversion**: Add support for converting Biolink's space-separated class names (e.g., "named thing") to CamelCase (e.g., "NamedThing").

2. **Slot Case Conversion**: Convert Biolink's space-separated slot names to snake_case automatically.

3. **Schema Documentation**: Generate comprehensive documentation for the KGX schema that includes both KGX-specific elements and inherited Biolink elements.

4. **Validation Rules**: Add KGX-specific validation rules to ensure data conforms to both KGX and Biolink requirements.

## References

- [Biolink Model](https://biolink.github.io/biolink-model/)
- [LinkML Documentation](https://linkml.io/linkml/)
- [LinkML Map Documentation](https://linkml.io/linkml-map/)