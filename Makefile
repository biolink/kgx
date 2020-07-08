export PYTHONPATH=.

tests: unit-tests integration-tests

unit-tests:
	[ -d $(MYDIR) ] || mkdir -p tests/target
	pytest tests/unit/*.py

integration-tests:
	[ -d $(MYDIR) ] || mkdir -p tests/target
	pytest tests/*.py

typecheck:
	mypy kgx --ignore-missing-imports
