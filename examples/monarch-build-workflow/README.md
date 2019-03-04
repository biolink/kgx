## Monarch Build Workflow

With the scripts in this directory we will build a Neo4j instance from clinvar, orphanet, omim, hpoa, hgnc, and SemMedDb.

Our Neo4j queries use apoc procedures, and the required apoc jar will be downloaded and placed in the `neo4j/plugins` directory with the `make neo4j-start` command, which will then start a Docker container. By default the Neo4j instance will be configured to not use authentication, and you may wish to change this. To stop the neo4j docker container use `make neo4j-stop`.

### Building the graph
All the following commands can be executed together with `make build-graph`.

First we will download all the needed files:
```
make download
```
Next transform the SemMedDb files so that they fit the biolink model, this takes about 30 min for me:
```
python scripts/transform_semmeddb.py
```
Finally, we will build the knowledge graph:
```
nohup python scripts/main.py &
```
This will take a very long time to run, on my computer it's about four hours. The vast majority of time will be spent on waiting for rdflib to load the ttl files. `scripts/main.py` will load each of the ttl files and transform them to json. The json files are lightening quick to re-load in comparison to the ttl files. Then, the CSV files from SemMedDb will be loaded. This will result in a massive NetworkX graph, which then will have its cliques merged. Finally, node categories and edge labels that do not match the biolink model will be discarded.

The resulting graph will be saved as `results/clique_merged.csv.tar`, and the json files will be in the `results/` directory along side it.

### Uploading to Neo4j

Use the `make move-results` command to unpack the tar file into the edge set and node set CSV files, and have them moved into `neo4j/import`.

TODO: Finish explaining the process of uploading to Neo4j
