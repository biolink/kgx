export PYTHONPATH=.

tests: unit-tests integration-tests

unit-tests:
	pytest tests/unit/*.py

integration-tests:
	pytest tests/*.py

typecheck:
	mypy kgx --ignore-missing-imports
