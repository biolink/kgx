test: unit-tests integration-tests

unit-tests:
	poetry run pytest tests/unit/test_source/*.py
	poetry run pytest tests/unit/test_sink/*.py
	poetry run pytest tests/unit/*.py


integration-tests:
	poetry run pytest tests/*.py

typecheck:
	poetry run mypy kgx --ignore-missing-imports
