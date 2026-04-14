.DEFAULT_GOAL := all
SHELL := bash
RUN := uv run
VERSION = $(shell git tag | tail -1)

.PHONY: all
all: install test

.PHONY: install
install:
	uv sync

.PHONY: test
test: unit-tests integration-tests

.PHONY: unit-tests
unit-tests:
	$(RUN) pytest tests/unit/test_source/*.py
	$(RUN) pytest tests/unit/test_sink/*.py
	$(RUN) pytest tests/unit/*.py

.PHONY: integration-tests
integration-tests:
	$(RUN) pytest tests/integration/*.py

.PHONY: docs
docs:
	cd docs && $(RUN) make html

# Clean up intermediate schema files
.PHONY: schema-clean
schema-clean:
	@echo "Cleaning up intermediate schema files..."
	rm -f kgx/schema/kgx_merged.yaml kgx/schema/kgx_final.yaml kgx/schema/derived_kgx_schema.yaml
	rm -f kgx/schema/transformations/full_transform.transform.yaml

# Merge imported schemas (biolink-model) into a single file
.PHONY: schema-merge
schema-merge: schema-clean
	@echo "Merging schemas..."
	$(RUN) gen-linkml --mergeimports --format yaml kgx/schema/kgx.yaml -o kgx/schema/kgx_merged.yaml

# Generate transformation specification
.PHONY: schema-transform
schema-transform: schema-merge
	@echo "Generating transformation specification..."
	$(RUN) python kgx/schema/scripts/generate_transform.py kgx/schema/kgx_merged.yaml kgx/schema/transformations/full_transform.transform.yaml

# Apply transformation to create final schema
.PHONY: schema-apply
schema-apply: schema-transform
	@echo "Applying transformation..."
	$(RUN) linkml-map derive-schema -T kgx/schema/transformations/full_transform.transform.yaml -o kgx/schema/kgx_complete.yaml kgx/schema/kgx_merged.yaml

# Complete schema build process
.PHONY: schema
schema: schema-apply
	@echo "Schema build complete. Final schema is at kgx/schema/kgx_complete.yaml"

.PHONY: clean
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]'`
	rm -rf .pytest_cache
	rm -rf dist
	rm -rf build
