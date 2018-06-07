export PYTHONPATH=.
CONTAINER_NAME=kgx_neo_test

NEO4J_ADDRESS=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password


test:
	pytest tests/*py

example:
	@echo "1. Validate"
	kgx validate tests/resources/semmed/cell.json tests/resources/semmed/gene.json tests/resources/semmed/protein.json

	@echo "\n2. Combine three files into one"
	kgx dump tests/resources/semmed/cell.json tests/resources/semmed/gene.json tests/resources/semmed/protein.json target/combined.json

	@echo "\n3. Uploading combined.json to a local neo4j instance"
	kgx neo4j-upload $(NEO4J_ADDRESS) $(NEO4J_USER) $(NEO4J_PASSWORD) target/combined.json

	@echo "\n4. Downloading a subset of what we had uploaded. Running in debug mode so we can see the cypher queries"
	kgx --debug neo4j-download --properties object id UMLS:C1290952 --labels subject disease_or_phenotypic_feature $(NEO4J_ADDRESS) $(NEO4J_USER) $(NEO4J_PASSWORD) target/neo4j-download.json

	@echo "\n5. Downloading another subset, this time filtering on the edge label"
	kgx --debug neo4j-download --labels edge predisposes $(NEO4J_ADDRESS) $(NEO4J_USER) $(NEO4J_PASSWORD) target/predisposes.json

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
