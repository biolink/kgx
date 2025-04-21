#!/usr/bin/env python3

from linkml_runtime.utils.schemaview import SchemaView
import yaml
import sys

def generate_transform_spec(schema_path, output_path):
    """Generate a comprehensive transformation spec with all classes and slots from the source schema."""
    
    # Load the schema
    view = SchemaView(schema_path)
    
    # Get all class names
    all_classes = view.all_classes()
    
    # Get all enum names
    all_enums = view.all_enums()
    
    # Create transformation spec
    transform_spec = {
        "id": "kgx-inheritance",
        "title": "KGX Inheritance Transformation",
        "description": "Adds Node as parent of named thing and Edge as parent of association",
        "prefixes": {
            "biolink": "https://w3id.org/biolink/vocab/",
            "kgx": "https://w3id.org/biolink/kgx/"
        },
        "source_schema": schema_path,
        "target_schema": output_path.replace(".transform.yaml", ".yaml"),
        "class_derivations": {},
        "enum_derivations": {}
    }

    # Add all classes to be copied
    for class_name in all_classes:
        transform_spec["class_derivations"][class_name] = {"populated_from": class_name}
        
    # Add all enums to be copied
    for enum_name in all_enums:
        transform_spec["enum_derivations"][enum_name] = {"populated_from": enum_name}
        
    # Add special overrides for named thing and association
    if "named thing" in transform_spec["class_derivations"]:
        transform_spec["class_derivations"]["named thing"]["overrides"] = {"is_a": "Node"}
    
    if "association" in transform_spec["class_derivations"]:
        transform_spec["class_derivations"]["association"]["overrides"] = {"is_a": "Edge"}
    
    # Write the transformation spec
    with open(output_path, 'w') as file:
        yaml.dump(transform_spec, file, sort_keys=False)
    
    print(f"Generated transformation spec with {len(all_classes)} classes and {len(all_enums)} enums")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python generate_transform.py <schema_path> <output_path>")
        sys.exit(1)
    
    schema_path = sys.argv[1]
    output_path = sys.argv[2]
    generate_transform_spec(schema_path, output_path)