NEO4J_CONTAINER_NAME=ncats-monarch-graph-2019-02-20

run:
	make install
	make download

install:
	pip install -r requirements.txt

unpack-semmeddb:
	tar -xvf data/transformed_semmeddb.csv.tar -C data/
	sudo mv data/edges.csv data/semmeddb_edges.csv
	sudo mv data/nodes.csv data/semmeddb_nodes.csv

move-results:
	tar -xvf results/clique_merged.csv.tar -C results/
	mv results/edges.csv results/clique_merged_edges.csv
	mv results/nodes.csv results/clique_merged_nodes.csv
	sudo mv results/clique_merged_nodes.csv neo4j/import/
	sudo mv results/clique_merged_edges.csv neo4j/import/

download:
	# Download data files
	wget --no-clobber http://purl.obolibrary.org/obo/ro.owl -O data/ro.owl
	wget --no-clobber https://data.monarchinitiative.org/ttl/clinvar.ttl -O data/clinvar.ttl
	wget --no-clobber https://data.monarchinitiative.org/ttl/orphanet.ttl -O data/orphanet.ttl
	wget --no-clobber https://data.monarchinitiative.org/ttl/omim.ttl -O data/omim.ttl
	wget --no-clobber https://archive.monarchinitiative.org/latest/ttl/hpoa.ttl -O data/hpoa.ttl
	wget --no-clobber https://data.monarchinitiative.org/ttl/hgnc.ttl -O data/hgnc.ttl
	wget --no-clobber http://34.229.55.225/nodes_neo4j.csv -O data/semmeddb_nodes.csv
	wget --no-clobber http://34.229.55.225/edges_neo4j.csv -O data/semmeddb_edges.csv
	# Download ontology files
	wget --no-clobber http://purl.obolibrary.org/obo/mondo.owl -O data/mondo.owl
	wget --no-clobber https://raw.githubusercontent.com/obophenotype/human-phenotype-ontology/master/hp.owl -O data/hp.owl
	wget --no-clobber https://raw.githubusercontent.com/The-Sequence-Ontology/SO-Ontologies/master/so.owl -O data/so.owl
	wget --no-clobber http://purl.obolibrary.org/obo/go.owl -O data/go.owl

neo4j-start:
	wget --no-clobber https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/download/3.0.8.6/apoc-3.0.8.6-all.jar --directory-prefix=neo4j/plugins
	echo `pwd`
	docker run \
		-d \
		--rm \
		--env NEO4J_dbms_memory_heap_maxSize=5120 \
		--name ${NEO4J_CONTAINER_NAME} \
		-p 8086:7474 \
		-p 8088:7473 \
		-p 8087:7687 \
		-v `pwd`/neo4j/plugins:/plugins \
		-v `pwd`/neo4j/data:/data \
		-v `pwd`/neo4j/conf:/var/lib/neo4j/conf \
		-v `pwd`/neo4j/import:/var/lib/neo4j/import \
		neo4j:3.0

neo4j-stop:
	docker stop ${NEO4J_CONTAINER_NAME}

neo4j-ssh:
	docker exec -it ${NEO4J_CONTAINER_NAME} /bin/bash

neo4j-logs:
	docker logs ${NEO4J_CONTAINER_NAME}
