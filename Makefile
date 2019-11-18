export PYTHONPATH=.
CONTAINER_NAME=kgx_neo_test

NEO4J_HOST=localhost
NEO4J_ADDRESS=bolt://$(NEO4J_HOST):7687
#NEO4J_ADDRESS=http://$(NEO4J_HOST):7474
NEO4J_USER=neo4j
NEO4J_PASSWORD=demo

clean:
	@-rm -f tests/logs/*.log
	@-rm -f target/*.json
	@echo "Environment cleaned."

tests:
	pytest tests/*py

.PHONY: examples tests

examples: target/combined.json
	@echo "Validate cell.json"
	kgx validate  --output_dir tests/logs tests/resources/semmed/cell.json

	@echo "Validate gene.json"
	kgx validate  --output_dir tests/logs tests/resources/semmed/gene.json

	@echo "Validate protein.json"
	kgx validate  --output_dir tests/logs tests/resources/semmed/protein.json

target/combined.json:
	@echo "Combine three files into one"
	kgx dump --output target/combined.json tests/resources/semmed/cell.json tests/resources/semmed/gene.json tests/resources/semmed/protein.json

typecheck:
	mypy kgx --ignore-missing-imports

neo4j_tests: start_neo4j run_neo_tests stop_neo4j

start_neo4j:
	@echo "Starting a Neo4j Docker container with name: ${CONTAINER_NAME}"
	docker run --rm --name $(CONTAINER_NAME) --detach --env NEO4J_AUTH=$(NEO4J_USER)/$(NEO4J_PASSWORD) --publish=7474:7474 --publish=7687:7687 --volume=`pwd`/docker_test_data:/data --volume=`pwd`/docker_test_logs:/logs neo4j
	@sleep 5

run_neo_tests: target/combined.json
	@echo "Running Neo4j tests"

	@echo "\nneo-1. Uploading xln.csv and xle.csv to a local neo4j instance"
	-python examples/scripts/load_csv_to_neo4j.py tests/resources/x1n.csv tests/resources/x1e.csv --host $(NEO4J_HOST)

	@echo "\nneo-2. Reading csv data back from the local neo4j instance"
	-python examples/scripts/read_from_neo4j.py --host $(NEO4J_HOST)

	@echo "\nneo-3. Uploading combined.json to a local neo4j instance"
	-kgx neo4j-upload --address $(NEO4J_ADDRESS) --username $(NEO4J_USER) --password $(NEO4J_PASSWORD) target/combined.json

	@echo "\nneo-4. Downloading a subset of what we had uploaded. Running in debug mode so we can see the cypher queries"
	#
	# neo4j-download no longer takes subject or object id's as possible filter inputs?
	#
	#-kgx --debug neo4j-download  --address $(NEO4J_ADDRESS) --username $(NEO4J_USER) --password $(NEO4J_PASSWORD) --properties object id UMLS:C1290952 --subject-label disease_or_phenotypic_feature --output target/neo4j-download.json
	#
	-kgx --debug neo4j-download  --address $(NEO4J_ADDRESS) --username $(NEO4J_USER) --password $(NEO4J_PASSWORD)  --subject-label disease_or_phenotypic_feature --output target/neo4j-download.json

	@echo "\nneo-5. Downloading another subset, this time filtering on the edge label"
	-kgx --debug neo4j-download  --address $(NEO4J_ADDRESS) --username $(NEO4J_USER) --password $(NEO4J_PASSWORD) --edge-type predisposes --output target/predisposes.json

docker_on:
    @echo "Stopping a Neo4j Docker container with name: ${CONTAINER_NAME}"

stop_neo4j: docker_on
	docker stop $(CONTAINER_NAME)
	@-rm -rf docker_test_data docker_test_logs
	@sleep 3
    @echo "Neo4j Docker container with name: ${CONTAINER_NAME} is stopped"
