# Changelog

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
