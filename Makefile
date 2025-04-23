RUN=poetry run
VERSION = $(shell git tag | tail -1)

test: unit-tests integration-tests

unit-tests:
	$(RUN) pytest tests/unit/test_source/*.py
	$(RUN) pytest tests/unit/test_sink/*.py
	$(RUN) pytest tests/unit/*.py


integration-tests:
	$(RUN) pytest tests/integration/*.py

docs:
	cd docs && $(RUN) make html

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
