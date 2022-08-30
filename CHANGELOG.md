# Changelog
## 1.7.0
- updates to infores usage according to Biolink-model changes
- bug fixes for infores auto-mapping

## 1.6.2
- allow infores to be submitted via input files and via knowledge_sources click parameter
- add some checks for dually submittied primary_knowledge_sources

## 1.6.1
- minor code clean up, doc clean up and zenodo release

## 1.6.0
- update provenance to work with Biolink3

## 1.5.9 (2022-06-13)
- make streaming transform default

## 1.5.8 (2022-06-02)
- take down logging level for neo4j

## 1.5.7 (2022-04-15)
- update click utils to use stream for transform in merge command

## 1.5.6 (2022-03-09)
- upgrade linkml & rdflib
- fix dependencies on black, sphinx, bmt

## 1.5.5 (2022-01-18)
- upgrade neo4j support to neo4j 4.3.0
- enhance logging support

## 1.5.3 (2021-09-14)
- remove pystache requirement

## 1.5.2 (2021-08-23)
- Update to bmt 0.7.4 
- add error reporting for empty lines in nt parser

## 1.5.1 (2021-08-23)

- Keep track of edge properties in owl source

## 1.5.0 (2021-08-12)

- Support mixins in ancestors hierarchy
- Invalidate mixins as categories
- Maintenance and bug fixes

## 1.4.0 (2021-07-15)

- This version replaces the previously hard-coded `config.yaml` Biolink Model release number used in the included Biolink Model Toolkit.
- Users of the KGX 'validate' functionality can also [reset the Biolink Model (SemVer) release number at the CLI, and programmatically, with the `Validator.set_biolink_model()` class method](./docs/reference/validator.md#biolink-model-versioning).
- KGX made [Biolink Model 2.0++ aware with respect to new provenance slots - `knowledge_source` and  its descendant slot definitions](./docs/reference/transformer.md#provenance-of-nodes-and-edges). These slot properties need to be explicitly specified in the `Transformer.transform` **input_args** dictionary.
- Some support for [heuristic auto-generation of candidate format-compliant CURIES Biolink Model 2.0++ Information Resource ("InfoRes") CURIES](./docs/reference/transformer.md#infores-identifier-rewriting). The new code also provides a regular expression and meta-data based rewrite of knowledge source names into such CURIES.
- Some additional clarification in the 'readTheDocs' documentation including for 1.3.0 streaming release features.
- Added tox testing and version support and refactored according to black and flake8 linting reports.

## 1.3.0 (2021-06-21)

- Add streaming option to validation

## 1.2.0 (2021-06-17)

- Update BMT to 0.7.2
- Update LinkML to 1.0.0
- Pin some major release versions in requirements.txt

## 1.1.0 (2021-05-24)

- Migrate to LinkML from BiolinkML

## 1.2.0 (2021-06-17)

- Update BMT to 0.7.2
- Update LinkML to 1.0.0
- Pin some major release versions in requirements.txt

## 1.1.0 (2021-05-24)

- Migrate to LinkML from BiolinkML

## 1.0.0 - (2021-03-16)

- Fix bug with caching records in RdfSource
- Add shortcuts for arguments in all CLI operations
- Fix usage in CLI


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
