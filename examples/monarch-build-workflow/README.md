## Monarch Build Workflow

With the scripts in this directory we will build a Neo4j instance from clinvar, orphanet, omim, hpoa, hgnc, and SemMedDb.

Our Neo4j queries use apoc procedures, and the required apoc jar will be downloaded and placed in the `neo4j/plugins` directory with the `make neo4j-start` command, which will then start a Docker container. By default the Neo4j instance will be configured to not use authentication, and you may wish to change this. To stop the neo4j docker container use `make neo4j-stop`.

### Building the graph
All the following commands can be executed together with `make run`. This will take a very long time, so it's recommended that you run with `nohup make run &`. This is an overview of the sub-commands that are called. This workflow was designed using Ubuntu, Windows users may have trouble running it.

First the projects dependencies are installed.
```
make install
```
Next all data files are downloaded. These are mostly turtle files from https://data.monarchinitiative.org/ttl/ files but there are also CSV files from the SemMedDb Neo4j instance.
```
make download
```
Next the downloaded SemMedDb files are transformed so that they fit the biolink model format that KGX is expecting.
```
python scripts/transform_semmeddb.py
```
Finally, we run the main script that builds the knowledge graph.
```
python scripts/main.py
```
This will take a very long time to run, on my computer it's about four hours. The vast majority of time will be spent on waiting for rdflib to load the ttl files. `scripts/main.py` will load each of the ttl files and transform them to CSV. The CSV files are lightening quick to re-load in comparison to the ttl files. Then all CSV files, including those from SemMedDb, will be loaded. This will result in a massive NetworkX graph, which then will have its cliques (sets of equivalent nodes) merged and its nodes and edges categorized.

The resulting graph will be saved as `results/clique_merged.csv.tar`, and the intermediate CSV files will be in the `results/` directory along side it.

### Uploading to Neo4j
Execute `make neo4j-start` to get Neo4j running. This will download an APOC plugin and then spin up a Docker instance of Neo4j.

Use the `make move-results` command to unpack the tar file into the edge set and node set CSV files, and have them moved into `neo4j/import`.

Finally, you can use `scripts/load.cql` to load the CSV file into Neo4j. There is no great way to do this. I've found the easiest way is to enter into the Docker container, and then execute the Neo4j shell, and then copy and paste the queries from `scripts/load.cql` into the terminal and execute them that way. But, if you can get access to the Neo4j shell from outside of the docker container, then you might try to point it directly at `scripts/load.cql` instead. Another option to try is to pass these queries to the Neo4j instance using its HTTP API. However you wish to do it, the queries in `scripts/load.cql` should be used to load the CSV files sitting in `neo4j/import`.
