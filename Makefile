export PYTHONPATH=.
CONTAINER_NAME=kgx_neo_test

test:
	pytest tests/*py

typecheck:
	mypy kgx --ignore-missing-imports

neo_tests: start_docker run_neo_tests stop_docker

start_docker:
	@echo "Starting a Neo4j docker container with name: ${CONTAINER_NAME}"
	docker run --name $(CONTAINER_NAME) --detach --env NEO4J_AUTH=neo4j/demo --publish=7474:7474 --publish=7687:7687 --volume=`pwd`/docker_test_data:/data --volume=`pwd`/docker_test_logs:/logs neo4j
	@sleep 5

run_neo_tests:
	@echo "Running neo tests"
	python examples/scripts/load_csv_to_neo4j.py tests/resources/x1n.csv tests/resources/x1e.csv
	python examples/scripts/read_from_neo4j.py

stop_docker:
	docker stop kgx_neo_test
	rm -rf docker_test_data docker_test_logs
