## Monarch Build Workflow

With the scripts in this directory we will build a Neo4j instance using clinvar, orphanet, omim, hpoa, and hgnc. First we will download the RDF files, then we will merge them into a single file, then we will merge cliques and choose appropriate identifiers. Finally, we will take the resulting RDF file and convert it to CSV, and then take steps to upload this CSV to Neo4j.

Our Neo4j queries use apoc procedures, and the required apoc jar will be downloaded and placed in the `neo4j/plugins` directory with the `make neo4j-start` command, which will then start a Docker container. By default the Neo4j instance will be configured to not use authentication, and you may wish to change this. To stop the neo4j docker container use `make neo4j-stop`.
