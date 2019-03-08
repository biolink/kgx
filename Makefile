export PYTHONPATH=.
CONTAINER_NAME=kgx_neo_test

NEO4J_ADDRESS=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

clean:
	@-cd tests/logs
	@-rm -f *.json *.log
	@echo "Environment cleaned."

tests:
	pytest tests/*py

.PHONY: examples tests

examples:
	@echo "1a. Validate cell.json"
	kgx validate  --output_dir tests/logs tests/resources/semmed/cell.json

	@echo "1b. Validate gene.json"
	kgx validate  --output_dir tests/logs tests/resources/semmed/gene.json

	@echo "1c. Validate protein.json"
	kgx validate  --output_dir tests/logs tests/resources/semmed/protein.json

	@echo "\n2. Combine three files into one"
	kgx dump --output tests/logs/combined.json tests/resources/semmed/cell.json tests/resources/semmed/gene.json tests/resources/semmed/protein.json

typecheck:
	mypy kgx --ignore-missing-imports

neo_tests: start_docker run_neo_tests stop_docker

start_docker:
	@echo "Starting a Neo4j docker container with name: ${CONTAINER_NAME}"
	docker run --name $(CONTAINER_NAME) --detach --env NEO4J_AUTH=neo4j/demo --publish=7474:7474 --publish=7687:7687 --volume=`pwd`/docker_test_data:/data --volume=`pwd`/docker_test_logs:/logs neo4j
	@sleep 5

run_neo_tests:
	@echo "Running neo tests"

	@echo "\nneo-1. Uploading xln.csv and xle.csv to a local neo4j instance"
	python examples/scripts/load_csv_to_neo4j.py tests/resources/x1n.csv tests/resources/x1e.csv

	@echo "\nneo-2. Reading csv data back from the local neo4j instance"
	python examples/scripts/read_from_neo4j.py

	@echo "\nneo-3. Uploading combined.json to a local neo4j instance"
	kgx neo4j-upload $(NEO4J_ADDRESS) $(NEO4J_USER) $(NEO4J_PASSWORD) target/combined.json

	@echo "\nneo-4. Downloading a subset of what we had uploaded. Running in debug mode so we can see the cypher queries"
	kgx --debug neo4j-download --properties object id UMLS:C1290952 --labels subject disease_or_phenotypic_feature $(NEO4J_ADDRESS) $(NEO4J_USER) $(NEO4J_PASSWORD) target/neo4j-download.json

	@echo "\nneo-5. Downloading another subset, this time filtering on the edge label"
	kgx --debug neo4j-download --labels edge predisposes $(NEO4J_ADDRESS) $(NEO4J_USER) $(NEO4J_PASSWORD) target/predisposes.json


stop_docker:
    @echo "Stopping a Neo4j docker container with name: ${CONTAINER_NAME}"
	docker stop kgx_neo_test
	rm -rf docker_test_data docker_test_logs
