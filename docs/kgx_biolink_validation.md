# KGX and Biolink Model JSON Schema Validation

## TL;DR - For the Skeptics

**KGX is simply JSON Schema validation of JSON Lines data conformant to the Biolink Model JSON Schema.**

That's it. Nothing more, nothing less. KGX JSON Lines format is just Biolink Model-compliant JSON objects, one per line.

---

## Understanding KGX Validation

### The Simple Truth

KGX doesn't invent a new schema. KGX uses the **official Biolink Model JSON Schema** to validate knowledge graph data. When you serialize data in KGX JSON Lines format, you are creating JSON objects that validate against:

**[Biolink Model JSON Schema](https://w3id.org/biolink/biolink-model/biolink-model.json)**

### How It Works

1. **Biolink Model** defines the schema for nodes (NamedThing) and edges (Association)
2. **KGX** serializes this data as:
   - **JSON**: Standard JSON with `nodes` and `edges` arrays
   - **JSON Lines**: One JSON object per line (streaming-friendly)
   - **TSV**: Tabular format with pipe-delimited multi-valued fields
   - **RDF**: Semantic web format

3. **Validation**: Each node and edge object conforms to the Biolink Model JSON Schema

### The Schema Hierarchy

```
Biolink Model JSON Schema (https://w3id.org/biolink/biolink-model/biolink-model.json)
    ↓
  Defines: NamedThing, Association, and all their descendants
    ↓
  KGX validates against these definitions
    ↓
  Your JSON Lines data: One NamedThing or Association per line
```

---

## Practical Examples

### What You Write (KGX JSON Lines)

**nodes.jsonl:**
```json
{"id":"HGNC:11603","name":"TBX4","category":["biolink:Gene"],"in_taxon":["NCBITaxon:9606"]}
```

**edges.jsonl:**
```json
{"subject":"HGNC:11603","predicate":"biolink:contributes_to","object":"MONDO:0005002","knowledge_level":"observation","agent_type":"manual_agent"}
```

### What Validates It

The [Biolink Model JSON Schema](https://w3id.org/biolink/biolink-model/biolink-model.json) defines:

- **Gene** (inherits from NamedThing)
  - Required: `id`, `category`
  - Properties: `name`, `symbol`, `in_taxon`, `xref`, etc.

- **Association** (base class for all edges)
  - Required: `subject`, `predicate`, `object`, `knowledge_level`, `agent_type`
  - Properties: `publications`, `primary_knowledge_source`, `category`, etc.

### Verification

You can validate your KGX JSON Lines data yourself using any JSON Schema validator:

```bash
# Validate a node against Biolink Model schema
cat nodes.jsonl | head -1 | \
  ajv validate -s https://w3id.org/biolink/biolink-model/biolink-model.json -d -
```

---

## Why JSON Lines?

JSON Lines (`.jsonl`) is simply newline-delimited JSON. Each line is a complete, valid JSON object.

**Advantages:**
- **Streaming**: Process one record at a time (memory-efficient for large KGs)
- **Parallel processing**: Split files and process chunks independently
- **Append-friendly**: Add new records without rewriting the entire file
- **Debugging**: Inspect individual records easily
- **Standard format**: Widely supported by data tools (pandas, spark, etc.)

**Still JSON Schema Compliant:**
Each line is a JSON object that validates against the Biolink Model schema. The newline delimiter doesn't change the schema validation—it just changes how we store multiple objects.

---

## The Schema Relationship

### Biolink Model JSON Schema Structure

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "definitions": {
    "NamedThing": {
      "type": "object",
      "properties": {
        "id": { "type": "string", "description": "CURIE identifier" },
        "category": { 
          "type": "array",
          "items": { "type": "string" },
          "description": "Biolink categories"
        },
        "name": { "type": "string" },
        ...
      },
      "required": ["id", "category"]
    },
    "Gene": {
      "allOf": [
        { "$ref": "#/definitions/NamedThing" },
        {
          "properties": {
            "in_taxon": {
              "type": "array",
              "items": { "type": "string" }
            },
            "symbol": { "type": "string" }
          }
        }
      ]
    },
    "Association": {
      "type": "object",
      "properties": {
        "subject": { "type": "string" },
        "predicate": { "type": "string" },
        "object": { "type": "string" },
        "knowledge_level": { 
          "type": "string",
          "enum": ["knowledge_assertion", "logical_entailment", ...]
        },
        "agent_type": {
          "type": "string", 
          "enum": ["manual_agent", "automated_agent", ...]
        }
      },
      "required": ["subject", "predicate", "object", "knowledge_level", "agent_type"]
    }
  }
}
```

### Your KGX Data Validates Against This

Every node in your `nodes.jsonl` file must validate against the appropriate `NamedThing` subclass (Gene, Disease, ChemicalEntity, etc.).

Every edge in your `edges.jsonl` file must validate against the appropriate `Association` subclass.

---

## Addressing Common Concerns

### "Why not just use the Biolink Model directly?"

**You are.** KGX is a toolkit that:
- Serializes Biolink Model-compliant data into various formats (JSON, TSV, RDF)
- Validates data against the Biolink Model schema
- Transforms between formats while maintaining Biolink compliance
- Provides utilities for working with Biolink-compliant knowledge graphs

### "Is KGX adding extra requirements?"

**No.** KGX follows the Biolink Model requirements exactly. If a property is required in Biolink Model, it's required in KGX. If it's optional in Biolink Model, it's optional in KGX.

KGX is intentionally lenient—it allows non-Biolink properties to support knowledge graph evolution and real-world data, but all Biolink properties follow the official specification.

### "What about those agent_type values in the old docs?"

**Fixed.** The documentation now correctly reflects the current Biolink Model `AgentTypeEnum` and `KnowledgeLevelEnum` values. See the [updated KGX format documentation](kgx_format.md).

### "How do I know my data is valid?"

Three ways:

1. **Use KGX toolkit**: `kgx validate` command checks Biolink compliance
2. **JSON Schema validator**: Validate directly against `https://w3id.org/biolink/biolink-model/biolink-model.json`
3. **LinkML validator**: Use the LinkML tools to validate against the Biolink Model YAML schema

---

## Resources

### Official Biolink Model Resources
- **JSON Schema**: [https://w3id.org/biolink/biolink-model/biolink-model.json](https://w3id.org/biolink/biolink-model/biolink-model.json)
- **YAML Schema**: [https://w3id.org/biolink/biolink-model.yaml](https://w3id.org/biolink/biolink-model.yaml)
- **Documentation**: [https://biolink.github.io/biolink-model/](https://biolink.github.io/biolink-model/)
- **GitHub**: [https://github.com/biolink/biolink-model](https://github.com/biolink/biolink-model)

### KGX Resources
- **KGX Format Specification**: [kgx_format.md](kgx_format.md)
- **KGX Schema Generation**: [kgx_schema_generation.md](kgx_schema_generation.md)
- **GitHub**: [https://github.com/biolink/kgx](https://github.com/biolink/kgx)

---

## Example Validation Workflow

### Step 1: Create KGX JSON Lines Data

**nodes.jsonl**
```json
{"id":"HGNC:11603","name":"TBX4","symbol":"TBX4","category":["biolink:Gene"],"in_taxon":["NCBITaxon:9606"],"in_taxon_label":"Homo sapiens"}
{"id":"MONDO:0005002","name":"chronic obstructive pulmonary disease","category":["biolink:Disease"]}
```

**edges.jsonl**
```json
{"id":"uuid:123","subject":"HGNC:11603","predicate":"biolink:contributes_to","object":"MONDO:0005002","knowledge_level":"knowledge_assertion","agent_type":"manual_agent","primary_knowledge_source":["infores:hgnc"],"publications":["PMID:12345678"]}
```

### Step 2: Validate Using KGX

```bash
# Validate the data
kgx validate --input-format jsonl nodes.jsonl edges.jsonl

# Transform and validate in one step
kgx transform --input-format jsonl --output-format json \
  --input-file nodes.jsonl --input-file edges.jsonl \
  --output-file output.json
```

### Step 3: Verify Against Biolink Schema (Optional)

```bash
# Using ajv (Another JSON Schema Validator)
npm install -g ajv-cli

# Validate individual records
cat nodes.jsonl | while read line; do
  echo "$line" | ajv validate \
    -s https://w3id.org/biolink/biolink-model/biolink-model.json \
    -d -
done
```

---

## Conclusion

**KGX = Biolink Model JSON Schema + Practical Serialization Formats**

- Uses official Biolink Model JSON Schema
- Provides multiple serialization formats (JSON, JSON Lines, TSV, RDF)
- Validates data against Biolink Model requirements
- No additional schema overhead
- Standard JSON Schema validation applies

**Bottom line**: If your JSON Lines data validates against the Biolink Model JSON Schema, it's valid KGX. If it doesn't, it isn't. Simple as that.

For detailed property requirements and examples, see the [KGX Format Specification](kgx_format.md).
