# ArangoDB Support for KGX

KGX supports reading from and writing to ArangoDB databases via the `arangodb-download` and `arangodb-upload` CLI commands, or programmatically through the `ArangoSource` and `ArangoSink` classes.

## Approach

ArangoDB uses collections as containers for vertex and edges, while Neo4j uses labels and relationship types as classifiers. In addition, in ArangoDB, edges are JSON documents in a collection with _from and _to fields. KGX adopts a convention for ArangoDB which accommodates this key difference.

### CURIE Reconstruction

KGX supports ArangoDB knowledge graph databases which use a convention where:

- **Node collections** are named by ontology prefix (e.g., `CL`, `UBERON`, `MONDO`)
- **Node `_key` values** are the ontology local ID (e.g., `1000300`)
- **Edge collections** are named by the subject and object ontology prefixes separated by a hyphen (e.g., `CL-UBERON`)
- **Edge `_from`/`_to` values** reference the appropriate node collections (e.g., `CL/1000300`, `UBERON/0001992`)

On **export**, KGX reconstructs full CURIEs from this convention:

- Node ID: `{collection}:{_key}` (e.g., `CL:1000300`)
- Edge subject: derived from `_from` (e.g., `CL/1000300` becomes `CL:1000300`)
- Edge object: derived from `_to` (e.g., `UBERON/0001992` becomes `UBERON:0001992`)

On **import** with `--curie-routing`, KGX reverses this process:

- Node CURIE prefix determines the target collection (e.g., `CL:1000300` goes to collection `CL` with `_key` `1000300`)
- Edge subject/object prefixes determine the edge collection (e.g., subject `CL:...` to object `UBERON:...` goes to collection `CL-UBERON`)

### Compatibility with Other Sources and Sinks

Since `arangodb-download` writes standard KGX format files (TSV, JSON, etc.), the output can be used directly as input to any other KGX sink — including Neo4j upload. The exported files contain fully-reconstructed CURIEs for node IDs and edge subject/object fields, which are valid KGX records for any downstream format.

Edge `subject` and `object` fields are reconstructed from `_from`/`_to` when not stored in the edge document. This is the case for databases which rely entirely on `_from`/`_to` for edge endpoints rather than storing `subject`/`object` as document fields.

The reverse direction (Neo4j download followed by ArangoDB upload) is also structurally compatible, but is **lossy**. `NeoSource` only extracts three fields per node (`id`, `name`, `category`) and four per edge (`subject`, `predicate`, `relation`, `object`); all other properties stored in Neo4j are dropped. By contrast, `ArangoSource` returns full documents. If `--curie-routing` is used on upload, node IDs must be proper CURIEs; if Neo4j nodes lack a stored `id` property, `NeoSource` falls back to Neo4j's internal integer node ID, which is not a CURIE and will not route correctly.

### Collection Discovery

Databases may contain many per-ontology collections. The `--all-collections` flag automatically discovers all non-system document and edge collections, eliminating the need to enumerate them manually.

## Supported Formats

Both `arangodb-download` and `arangodb-upload` support all standard KGX formats — not just TSV.

**Download output formats:** `tsv`, `csv`, `json`, `jsonl`, `nt`, `jelly`, `parquet`, `sql`, `neo4j`, `arangodb`, `graph`, `null`

**Upload input formats:** `tsv`, `csv`, `json`, `jsonl`, `obojson`, `obo-json`, `trapi-json`, `nt`, `jelly`, `owl`, `parquet`, `duckdb`, `sssom`, `neo4j`, `arangodb`, `graph`

## CLI Usage

### Download (Export)

Export all collections from a database to TSV:

```bash
kgx arangodb-download \
  -l http://localhost:8529 \
  -d database \
  -u root \
  -p password \
  -o /tmp/ontologies_export \
  -f tsv \
  --all-collections
```

Export specific collections:

```bash
kgx arangodb-download \
  -l http://localhost:8529 \
  -d database \
  -u root \
  -p password \
  -o /tmp/cl_uberon_export \
  -f tsv \
  --node-collection CL \
  --node-collection UBERON \
  --edge-collection CL-UBERON \
  --edge-collection CL-CL
```

### Upload (Import)

Import into flat `nodes`/`edges` collections (default behavior):

```bash
kgx arangodb-upload \
  /tmp/ontologies_export_nodes.tsv /tmp/ontologies_export_edges.tsv \
  -i tsv \
  -l http://localhost:8529 \
  -d database \
  -u root \
  -p password
```

Import with per-CURIE-prefix collection routing (reconstructs original structure):

```bash
kgx arangodb-upload \
  /tmp/ontologies_export_nodes.tsv /tmp/ontologies_export_edges.tsv \
  -i tsv \
  -l http://localhost:8529 \
  -d database \
  -u root \
  -p password \
  --curie-routing
```

### Options Reference

**arangodb-download:**

| Option | Description |
|---|---|
| `-l`, `--uri` | ArangoDB URI (e.g., `http://localhost:8529`) |
| `-d`, `--database` | Database name |
| `-u`, `--username` | Username |
| `-p`, `--password` | Password |
| `-o`, `--output` | Output file path prefix |
| `-f`, `--output-format` | Output format (`tsv`, `json`, `jsonl`, etc.) |
| `--output-compression` | Output compression type |
| `-s`, `--stream` | Parse as a stream |
| `--node-collection` | Vertex collection name (repeatable) |
| `--edge-collection` | Edge collection name (repeatable) |
| `--all-collections` | Auto-discover and export all non-system collections |
| `-n`, `--node-filters` | Node filters (key value pair) |
| `-e`, `--edge-filters` | Edge filters (key value pair) |

**arangodb-upload:**

| Option | Description |
|---|---|
| `-i`, `--input-format` | Input format (`tsv`, `json`, `jsonl`, etc.) |
| `-c`, `--input-compression` | Input compression type |
| `-l`, `--uri` | ArangoDB URI |
| `-d`, `--database` | Database name |
| `-u`, `--username` | Username |
| `-p`, `--password` | Password |
| `-s`, `--stream` | Parse as a stream |
| `--node-collection` | Default vertex collection name (default: `nodes`) |
| `--edge-collection` | Default edge collection name (default: `edges`) |
| `--curie-routing` | Route to per-CURIE-prefix collections |
| `-n`, `--node-filters` | Node filters (key value pair) |
| `-e`, `--edge-filters` | Edge filters (key value pair) |

## Programmatic Usage

### Export

```python
from kgx.cli.cli_utils import arango_download

transformer = arango_download(
    uri="http://localhost:8529",
    database="database",
    username="root",
    password="password",
    output="/tmp/export",
    output_format="tsv",
    output_compression=None,
    stream=False,
    all_collections=True,
)
```

### Import

```python
from kgx.cli.cli_utils import arango_upload

transformer = arango_upload(
    inputs=["/tmp/export_nodes.tsv", "/tmp/export_edges.tsv"],
    input_format="tsv",
    input_compression=None,
    uri="http://localhost:8529",
    database="database",
    username="root",
    password="password",
    stream=False,
    curie_routing=True,
)
```

## Dependencies

The `python-arango` package is required:

```bash
poetry install  # python-arango is included in pyproject.toml
```
