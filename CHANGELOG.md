# Changelog

## 1.0.0b0 - (2021-03-11)

- Fixed caching bug in RdfSource
- Fixed bug when setting 'provided_by' for a source


## 1.0.0a0 - (2021-03-04)

- Added Source implementation for reading from various data stores
- Added Sink implementation for writing to various data stores
- Added a simplified Transformer that is agnostic
- Refactor KGX to make use of Sources and Sinks
- Added ability to stream graphs (Thanks to @gregr for the inspiration)


## 0.5.0 - (2021-02-23)

- This is the final release on the 0.x.x
- Added ability to assign a default category in SssomTransformer when the incoming category is invalid
- Added the ability to generate different types of graph summary reports


## 0.4.0 - (2021-02-08)

- Fixed a bug in NtTransformer when parsing malformed triples
- Added a transformer for parsing SSSOM
- Added ability to fold predicates to node properties
- Added ability to unfold node properties to predicates
- Added ability to remove singleton nodes from a graph
- Fixed bug in KGX CLI transform operation that prevented stack trace from showing up
- Unified the way predicates are parsed across different transformers
- Added ability to annotate edges with OWLStar vocabulary in RdfOwlTransformer


## 0.3.1 - (2021-01-20)

- Fixed a bug that led to import errors
- Fixed column ordering when exporting CSV/TSV
- Added ability to generate a TRAPI knowledge map for a graph
- Fixed bug in ObographJsonTransformer when inferring node category


## 0.3.0 - (2020-12-21)

- Added Graph API to KGX
- Added compatibility to Biolink Model 1.4.0
- Added the ability to facet on node and edge properties for graph-summary CLI operation
- Fixed a bug where source name defined in YAML was not being used for graph name


## 0.2.4 - (2020-11-25)

- Fixed bug when handling empty elements returned from bmt
- Fixed bug in CLI when setting property types in RdfTransformer
- Fixed bug when reifying edges on export in RdfTransformer

## 0.2.3 - (2020-11-04)

- Updated requirements.txt
- Added a Dockerfile

## 0.2.2 - (2020-11-04)

- Made get_toolkit method configurable
- Use proper input format when parsing OWL via CLI
- Switched default parsing format from CSV to TSV
- Fixed bug with spawning processes when transforming source
- Fixed parsing of xrefs from from OBOGraph JSON

## 0.2.1 - (2020-11-04) 

## 0.2.0 - (2020-10-20)

- Improved clique merge operation and addded support for strict and relaxed mode
- Fixed a major bug while merging lists during graph merge operation
- Fixed a bug in neo4j-download CLI
- Updated the merge and transform YAML format
- Added support for parsing and exporting jsonlines
- KGX now assigns UUID for edges that don't have and id
- Updated documentation

## 0.1.0 - (2020-09-10)

- Initial release
