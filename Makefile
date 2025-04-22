RUN=poetry run
VERSION = $(shell git tag | tail -1)

test: unit-tests integration-tests

unit-tests:
	poetry run pytest tests/unit/test_source/*.py
	poetry run pytest tests/unit/test_sink/*.py
	poetry run pytest tests/unit/*.py


integration-tests:
	poetry run pytest tests/integration/*.py

docs:
	cd docs && $(RUN) make html
