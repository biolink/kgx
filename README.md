# knowledge graph exchange
[![Build Status](https://travis-ci.org/NCATS-Tangerine/kgx.svg?branch=master)](https://travis-ci.org/NCATS-Tangerine/kgx)

KGX (Knowledge Graph Exchange) is a Python library and set of command line utilities for exchanging Knowledge Graphs (KGs) that conform to or are aligned to the [Biolink Model](https://biolink.github.io/biolink-model/).

The core datamodel is a [Property Graph](https://neo4j.com/developer/graph-database/) (PG), represented internally in Python using a [networkx MultiDiGraph model](https://networkx.github.io/documentation/stable/reference/classes/generated/networkx.MultiDiGraph.edges.html).

KGX allows conversion to and from:

 * RDF serializations (read/write) and SPARQL endpoints (read)
 * Neo4J endpoints (read) or Neo4J dumps (write)
 * CSV/TSV
 * Any format supported by networkx
 
KGX will also provide validation, to ensure the KGs are conformant to the Biolink model: making sure nodes are categorized using biolink classes, edges are labeled using valid biolink relationship types, and valid properties are used.

For additional background see the [Translator Knowledge Graph Drive](http://bit.ly/tr-kg)

## Installation

### Python 3.7 Version and Core Tool Dependencies

_Note that the installation of KGX requires Python 3.7 or better._  You should first confirm what version of Python 
you have running and upgrade to 3.7 as necessary, following best practices in your operating system. It is also 
assumed that the common development tools are installed as below: git, pip, etc. Again, install as needed following 
the instructions for your operating system. 

### Getting the Project Code

Go to where you wish to host your local project repository and git clone the project, namely:

```
cd /path/to/your/local/git/project/folder
git clone https://github.com/NCATS-Tangerine/kgx.git

# ... then  enter  into the cloned project repository
cd kgx
```

### Configuring a Safe Virtual Environment for KGX

Then, for convenience, make use of the `venv` module in Python 3 to create a lightweight virtual environment. Note that 
you may also have to install the appropriate `venv` package for Python 3.7. 

For example, under Ubuntu Linux, you might 

```
sudo apt install python3.7-venv  
```

Once `venv` is available, type:

```
python3 -m venv venv
source venv/bin/activate
```

To exit the environment, type:

```  
deactivate
```

To reenter, source the _activate_ command again.

Alternately, you can also use use **conda env** to manage packages and the development environment:

```
conda create -n translator-modules python=3.7
conda activate translator-modules
```

Some IDE's (e.g. PyCharm) may also have provisions for directly creating a virtual environment. This should work fine.

### Installing Python Dependencies 

Finally, the Python dependencies of the application need to be installed into the local environment using a version of 
`pip` matched to your Python 3.7 installation (assumed here to be called `pip3`). Again, follow the specific directives 
of your operating system for the installation. 

For example, under Ubuntu Linux, to install the Python 3.7 matched version of pip, type the following:

```
sudo apt install python3-pip
```

which will install the `pip3` command.  

At this point, it is advisable to separately install the `wheel` package dependency before proceeding further 
(Note: it is  assumed here that your `venv` is activated)

```
pip3 install wheel
```
 
After installation of the `wheel` package, we install the remaining KGX Python package dependencies without error:

```
pip3 install .
```

It is *sometimes* better to use the 'python -m pip' version of pip rather than just 'pip'
to ensure that the proper version of pip - i.e. for the python3 in your virtual environment - is used 
(i.e. once again, better check your pip version.  On some systems, it may run the operating system's version, 
which may not be compatible with your `venv` installed Python 3.7)

```
python -m pip install .
```

## Docker Dependencies

Some components of KGX leverage the use of Docker. If not installed in your Operating system environment, the following
[instructions to install Docker](DOCKER_README.md) may be followed to install it.

## Testing the Installation

Ensure that your `venv` environment is (still) activated (won't be after machine or terminal reboots!).
 
To test the basic _kgx_ application, run the following:

```
make tests
```

To test the use of the Neo4j database (using a Docker container), run  the following:

``` 
make neo4j_tests
```

To clean up the environment:

``` 
make clean
```

## Command Line Usage

Use the `--help` flag with any command to view documentation. See the [makefile](/Makefile) for examples, and run them with:

```
make examples
```

### Neo4j Upload
The `neo4j-upload` command takes any number of input files, builds a [networkx](https://networkx.github.io/) graph from them, and uploads that graph to a [neo4j](https://neo4j.com/) database. To do this it of course needs the database address, username, and password. This will only work through [bolt](https://neo4j.com/docs/operations-manual/current/configuration/connectors/). By default you can access a local neo4j instance at the address `bolt://localhost:7687`.
```
Usage: kgx neo4j-upload (-a|--address) ADDRESS [(-u|--username) USERNAME] [(-p|--password) PASSWORD] [OPTIONS] INPUTS...
```
The `--input-type` option can be used to specify the format of these files: csv, ttl, json, txt, graphml, rq, tsv.

### Neo4j Download
The `neo4j-download` command downloads a neo4j instance, builds a networkx graph from it, and saves it to the specified file. Like the upload command, this will only work through bolt.
```
Usage: kgx neo4j-download (-a|--address) ADDRESS [(-u|--username) USERNAME] [(-p|--password) PASSWORD] [OPTIONS] OUTPUT
```
The `--output-type` option can be used to specify the format of these files: csv, ttl, json, txt, graphml, rq, tsv. The `--labels` and `--properties` options allow for filtering on node and edge labels and properties.

The labels filter takes two inputs. The first input is a choice of where to apply the filter: subject, object, edge, node. The second is the label to apply.
```
--labels edge causes
```
This will result in searching for triples of the form: `(s)-[r:causes]-(o)`

The properties filter takes three inputs: the first being a choice of where to apply the filter, the second being the name of the property, and the third being the value of the property.
```
--properties subject name FANC
```
This will result in searching for triples of the form: `(s {name: "FANC"})-[r]-(o)`. These filter options can be given multiple times.

The `--directed` flag enforces the subject -> object edge direction.

The batch options allow you to download into multiple files. The `--batch-size` option determines the number of entries in each file, and the `--batch-start` determines which batch to start on.

### Validate
The `validate` command loads any number of files into a graph and checks that they adhere to the [TKG](https://github.com/NCATS-Tangerine/translator-knowledge-graph) standard.
```
Usage: kgx validate  --output_dir DIRECTORY [OPTIONS] INPUTS...
```
The `--input-type` option can be used to specify the format of these files: csv, ttl, json, txt, graphml, rq, tsv.

### Dump
The `dump` command takes any number of input file paths (all with the same file format), and outputs a file in the desired format.

```
Usage: kgx dump --output OUTPUT_PATH [OPTIONS] INPUTS... OUTPUT
```
The format will be inferred from the file extention. But if  this cannot be done then the `--input-type` and `--output-type` flags are useful to enforce a particular format. The following formats are supported: csv, tsv, txt (pipe delimited text), json, rq, graphml, ttl.
> *Note:* CSV/TSV representation require two files, one that represents the vertex set and one for the edge set. JSON, TTL, and GRAPHML files represent a whole graph in a single file. For this reason when creating CSV/TSV representation we will zip the resulting files in a .tar file.

The `dump` command can also be used to relabel nodes. This is particularly useful for ensuring that the CURIE identifier of each node reflects its category (e.g. genes having NCBIGene identifiers, proteins having UNIPROT identifiers, and so on). The `--mapping` option can be used to apply a pre-loaded mapping to the output as it gets transformed. If the `--preserve` flag is used then the old labels will be preserved under a modified name. Mappings are loaded with the load-mapping command.

### Load Mapping
A mapping is just a python [dict](https://docs.python.org/2/tutorial/datastructures.html#dictionaries) object. The `load-mapping` command builds a mapping out of the given CSV file, and saves it with the given name. That name can then be used with the `dump` commands `--mapping` option to apply the mapping.
```
Usage: kgx load-mapping [OPTIONS] NAME CSV
```
By default the command will treat the first and second columns as the input and output values for the mapping. But you can use the `--columns` option to specify which columns to use. The first and second integers provided will be the indexes of the input and output columns.
> **Note:** the columns are zero indexed, so the first is 0 and the second is 1, and so on.

The `--show` flag can be used to display a slice of the mapping when it is loaded so that the user can see which columns have been used. The `--no-header` flag can be used to indicate that the given CSV file does not have a header. If this flag is used then the first row will be used, otherwise it will be ignored.

#### Example:
First we load a mapping from a CSV file.
```
$ kgx load-mapping --show --columns 0 1 a_to_b_mapping tests/resources/mapping/mapping.csv
a58 : b58
a77 : b77
a17 : b17
a28 : b28
a92 : b92
Mapping 'a_to_b_mapping' saved at /home/user/.config/translator_kgx/a_to_b_mapping.pkl
```
Then we apply this mapping with the dump command.
```
kgx dump --mapping a_to_b_mapping tests/resources/mapping/nodes.csv target/mapping-out.json
Performing mapping: a_to_b_mapping
File created at: target/mapping-out.json
```

### Load and Merge
The `load-and-merge` command loads nodes and edges from knowledge graphs as defined in a config YAML, and merges them into a single graph. The destination URI, username, and password can be set with the `--destination-uri`, `--destination-username`, `--destination-password` options.


## Internal Representation

Internal representation is networkx MultiDiGraph which is a property graph.

The structure of this graph is expected to conform to the [tr-kg
standard](http://bit.ly/tr-kg-standard), briefly summarized here:

 * [Nodes](https://biolink.github.io/biolink-model/docs/NamedThing.html)
    * id : required
    * name : string
    * category : string. broad high level type. Corresponds to label in neo4j
    * extensible other properties, depending on
 * [Edges](https://biolink.github.io/biolink-model/docs/Association.html)
    * subject : required
    * predicate : required
    * object : required
    * extensible other fields

## Serialization/Deserialization

Intended to support

 - Generic Graph Formats
 - local or remote files
    - CSV
    - TSV (such as the [RKB adapted data loading formats](https://github.com/NCATS-Tangerine/translator-knowledge-graph/blob/develop/database/scripts/README.md))
    - RDF (Monarch/OBAN style, ...)
    - GraphML
    - CX
 - remote store via query API
    - neo4j/bolt
    - RDF


## RDF

## Neo4J

Neo4j implements property graphs out the box. However, some
implementations use reification nodes. The transform should allow for
de-reification.

# Running kgx with Neo4j in Docker

The _kgx_ tool can be run against a Neo4j database,  most conveniently run as a Docker container.  First, you need to install docker on your system (if you don't yet have it).

## Installation of Docker

To run Docker, you'll obviously need to [install Docker first](https://docs.docker.com/engine/installation/) in your target Linux operating environment (bare metal server or virtual machine running Linux).

For our installations, we typically use Ubuntu Linux, for which there is an [Ubuntu-specific docker installation using the repository](https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/#install-using-the-repository).
Note that you should have 'curl' installed first before installing Docker:

```
$ sudo apt-get install curl
```

For other installations, please find instructions specific to your choice of Linux variant, on the Docker site.

## Testing Docker

In order to ensure that Docker is working correctly, run the following command:

```
$ sudo docker run hello-world
```

This should result in something akin to the following output:

```
Unable to find image 'hello-world:latest' locally
latest: Pulling from library/hello-world
ca4f61b1923c: Pull complete
Digest: sha256:be0cd392e45be79ffeffa6b05338b98ebb16c87b255f48e297ec7f98e123905c
Status: Downloaded newer image for hello-world:latest

Hello from Docker!
This message shows that your installation appears to be working correctly.

To generate this message, Docker took the following steps:
 1. The Docker client contacted the Docker daemon.
 2. The Docker daemon pulled the "hello-world" image from the Docker Hub.
    (amd64)
 3. The Docker daemon created a new container from that image which runs the
    executable that produces the output you are currently reading.
 4. The Docker daemon streamed that output to the Docker client, which sent it
    to your terminal.

To try something more ambitious, you can run an Ubuntu container with:
 $ docker run -it ubuntu bash

Share images, automate workflows, and more with a free Docker ID:
 https://cloud.docker.com/

For more examples and ideas, visit:
 https://docs.docker.com/engine/userguide/
```

# Initializing and Testing the Docker neo4j Container

This may be done with the command:

```
make neo4j_tests

```

which should initialize and test the docker container, then turn it off. Note that if the tests fail, the make will not
  be completed and # knowledge graph exchange
[![Build Status](https://travis-ci.org/NCATS-Tangerine/kgx.svg?branch=master)](https://travis-ci.org/NCATS-Tangerine/kgx)

A utility library and set of command line tools for exchanging data in knowledge graphs.

The tooling here is partly generic but intended primarily for building
the
[translator-knowledge-graph](https://github.com/NCATS-Tangerine/translator-knowledge-graph).

For additional background see the [Translator Knowledge Graph Drive](http://bit.ly/tr-kg)

## Installation
The installation requires Python 3.

Go to where you wish to host your local project repository and git clone the project, namely:

```
cd /path/to/your/git/repo
git clone https://github.com/NCATS-Tangerine/kgx.git .

# ... then  enter  into your cloned project repository
cd kgx
```

Then, for convenience, make use of the `venv` module in Python 3 to create a lightweight virtual environment:
```
python3 -m venv venv
source env/bin/activate
```

**Note:** Be sure to set `PYTHONPATH`

The above script (properly setting the PYTHONPATH) can be found in [`environment.sh`](environment.sh). 

Either make the script executable (for future convenience in execution):

```
chmod u+x environment.sh
./environment.sh
```

or source the file:

```
source environment.sh
```

Finally, the Python dependencies of the application need to be installed into the local environment using the following command:

```
pip install .
```

To test the basic _kgx_ application, run the following:

```
make tests
```

To test the use of the Neo4j database (using a Docker container), run  the following:

``` 
make neo4j_tests
```

To clean the environment:

``` 
make clean
```

## Command Line Usage

Use the `--help` flag with any command to view documentation. See the [makefile](/Makefile) for examples, and run them with:

```
make examples
```

### Neo4j Upload
The `neo4j-upload` command takes any number of input files, builds a [networkx](https://networkx.github.io/) graph from them, and uploads that graph to a [neo4j](https://neo4j.com/) database. To do this it of course needs the database address, username, and password. This will only work through [bolt](https://neo4j.com/docs/operations-manual/current/configuration/connectors/). By default you can access a local neo4j instance at the address `bolt://localhost:7687`.
```
Usage: kgx neo4j-upload (-a|--address) ADDRESS [(-u|--username) USERNAME] [(-p|--password) PASSWORD] [OPTIONS] INPUTS...
```
The `--input-type` option can be used to specify the format of these files: csv, ttl, json, txt, graphml, rq, tsv.

### Neo4j Download
The `neo4j-download` command downloads a neo4j instance, builds a networkx graph from it, and saves it to the specified file. Like the upload command, this will only work through bolt.
```
Usage: kgx neo4j-download (-a|--address) ADDRESS [(-u|--username) USERNAME] [(-p|--password) PASSWORD] [OPTIONS] OUTPUT
```
The `--output-type` option can be used to specify the format of these files: csv, ttl, json, txt, graphml, rq, tsv. The `--labels` and `--properties` options allow for filtering on node and edge labels and properties.

The labels filter takes two inputs. The first input is a choice of where to apply the filter: subject, object, edge, node. The second is the label to apply.
```
--labels edge causes
```
This will result in searching for triples of the form: `(s)-[r:causes]-(o)`

The properties filter takes three inputs: the first being a choice of where to apply the filter, the second being the name of the property, and the third being the value of the property.
```
--properties subject name FANC
```
This will result in searching for triples of the form: `(s {name: "FANC"})-[r]-(o)`. These filter options can be given multiple times.

The `--directed` flag enforces the subject -> object edge direction.

The batch options allow you to download into multiple files. The `--batch-size` option determines the number of entries in each file, and the `--batch-start` determines which batch to start on.

### Validate
The `validate` command loads any number of files into a graph and checks that they adhere to the [TKG](https://github.com/NCATS-Tangerine/translator-knowledge-graph) standard.
```
Usage: kgx validate  --output_dir DIRECTORY [OPTIONS] INPUTS...
```
The `--input-type` option can be used to specify the format of these files: csv, ttl, json, txt, graphml, rq, tsv.

### Dump
The `dump` command takes any number of input file paths (all with the same file format), and outputs a file in the desired format.

```
Usage: kgx dump --output OUTPUT_PATH [OPTIONS] INPUTS... OUTPUT
```
The format will be inferred from the file extention. But if  this cannot be done then the `--input-type` and `--output-type` flags are useful to enforce a particular format. The following formats are supported: csv, tsv, txt (pipe delimited text), json, rq, graphml, ttl.
> *Note:* CSV/TSV representation require two files, one that represents the vertex set and one for the edge set. JSON, TTL, and GRAPHML files represent a whole graph in a single file. For this reason when creating CSV/TSV representation we will zip the resulting files in a .tar file.

The `dump` command can also be used to relabel nodes. This is particularly useful for ensuring that the CURIE identifier of each node reflects its category (e.g. genes having NCBIGene identifiers, proteins having UNIPROT identifiers, and so on). The `--mapping` option can be used to apply a pre-loaded mapping to the output as it gets transformed. If the `--preserve` flag is used then the old labels will be preserved under a modified name. Mappings are loaded with the load-mapping command.

### Load Mapping
A mapping is just a python [dict](https://docs.python.org/2/tutorial/datastructures.html#dictionaries) object. The `load-mapping` command builds a mapping out of the given CSV file, and saves it with the given name. That name can then be used with the `dump` commands `--mapping` option to apply the mapping.
```
Usage: kgx load-mapping [OPTIONS] NAME CSV
```
By default the command will treat the first and second columns as the input and output values for the mapping. But you can use the `--columns` option to specify which columns to use. The first and second integers provided will be the indexes of the input and output columns.
> **Note:** the columns are zero indexed, so the first is 0 and the second is 1, and so on.

The `--show` flag can be used to display a slice of the mapping when it is loaded so that the user can see which columns have been used. The `--no-header` flag can be used to indicate that the given CSV file does not have a header. If this flag is used then the first row will be used, otherwise it will be ignored.

#### Example:
First we load a mapping from a CSV file.
```
$ kgx load-mapping --show --columns 0 1 a_to_b_mapping tests/resources/mapping/mapping.csv
a58 : b58
a77 : b77
a17 : b17
a28 : b28
a92 : b92
Mapping 'a_to_b_mapping' saved at /home/user/.config/translator_kgx/a_to_b_mapping.pkl
```
Then we apply this mapping with the dump command.
```
kgx dump --mapping a_to_b_mapping tests/resources/mapping/nodes.csv target/mapping-out.json
Performing mapping: a_to_b_mapping
File created at: target/mapping-out.json
```

### Load and Merge
The `load-and-merge` command loads nodes and edges from knowledge graphs as defined in a config YAML, and merges them into a single graph. The destination URI, username, and password can be set with the `--destination-uri`, `--destination-username`, `--destination-password` options.


## Internal Representation

Internal representation is networkx MultiDiGraph which is a property graph.

The structure of this graph is expected to conform to the [tr-kg
standard](http://bit.ly/tr-kg-standard), briefly summarized here:

 * [Nodes](https://biolink.github.io/biolink-model/docs/NamedThing.html)
    * id : required
    * name : string
    * category : string. broad high level type. Corresponds to label in neo4j
    * extensible other properties, depending on
 * [Edges](https://biolink.github.io/biolink-model/docs/Association.html)
    * subject : required
    * predicate : required
    * object : required
    * extensible other fields

## Serialization/Deserialization

Intended to support

 - Generic Graph Formats
 - local or remote files
    - CSV
    - TSV (such as the [RKB adapted data loading formats](https://github.com/NCATS-Tangerine/translator-knowledge-graph/blob/develop/database/scripts/README.md))
    - RDF (Monarch/OBAN style, ...)
    - GraphML
    - CX
 - remote store via query API
    - neo4j/bolt
    - RDF


## RDF

## Neo4J

Neo4j implements property graphs out the box. However, some
implementations use reification nodes. The transform should allow for
de-reification.

# Running kgx with Neo4j in Docker

The _kgx_ tool can be run against a Neo4j database,  most conveniently run as a Docker container.  First, you need to install docker on your system (if you don't yet have it).

## Installation of Docker

To run Docker, you'll obviously need to [install Docker first](https://docs.docker.com/engine/installation/) in your target Linux operating environment (bare metal server or virtual machine running Linux).

For our installations, we typically use Ubuntu Linux, for which there is an [Ubuntu-specific docker installation using the repository](https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/#install-using-the-repository).
Note that you should have 'curl' installed first before installing Docker:

```
$ sudo apt-get install curl
```

For other installations, please find instructions specific to your choice of Linux variant, on the Docker site.

## Testing Docker

In order to ensure that Docker is working correctly, run the following command:

```
$ sudo docker run hello-world
```

This should result in something akin to the following output:

```
Unable to find image 'hello-world:latest' locally
latest: Pulling from library/hello-world
ca4f61b1923c: Pull complete
Digest: sha256:be0cd392e45be79ffeffa6b05338b98ebb16c87b255f48e297ec7f98e123905c
Status: Downloaded newer image for hello-world:latest

Hello from Docker!
This message shows that your installation appears to be working correctly.

To generate this message, Docker took the following steps:
 1. The Docker client contacted the Docker daemon.
 2. The Docker daemon pulled the "hello-world" image from the Docker Hub.
    (amd64)
 3. The Docker daemon created a new container from that image which runs the
    executable that produces the output you are currently reading.
 4. The Docker daemon streamed that output to the Docker client, which sent it
    to your terminal.

To try something more ambitious, you can run an Ubuntu container with:
 $ docker run -it ubuntu bash

Share images, automate workflows, and more with a free Docker ID:
 https://cloud.docker.com/

For more examples and ideas, visit:
 https://docs.docker.com/engine/userguide/
```

# Initializing and Testing the Docker neo4j Container

This may be done with the command:

```
make neo4j_tests

```

which should initialize and test the docker container, then turn it off. Note that if the tests fail, the make will not
  be completed with the docker container left running, hence you may need to manually stop the container (see below). 

After running this, you may restart the local test neo4j container with the command:

```
make start_neo4j
```

It will be running as the docker container named "kgx_neo_test" and visible on http://localhost:7474 (queries also going to bolt://localhost:7687).
Note that you may override the container name by settng the make command line parameter **CONTAINER_NAME**, for example:

```
make start_neo4j CONTAINER_NAME="MyNeo4jContainer"

```

The container may be stopped with:

```
make stop_neo4j
```

Or directly with with the _docker stop_ command (if you provided the **CONTAINER_NAME** when starting, then you need to 
supply it again).

Note that either way, the container is deleted (due ot the use of the docker ```--rm``` flag during start up).the docker container left running hence you may  need 

After running this, you may restart the local test neo4j container with the command:

```
make start_neo4j
```

It will be running as the docker container named "kgx_neo_test" and visible on http://localhost:7474 (queries also going to bolt://localhost:7687).
Note that you may override the container name by settng the make command line parameter **CONTAINER_NAME**, for example:

```
make start_neo4j CONTAINER_NAME="MyNeo4jContainer"

```

The container may be stopped with:

```
make stop_neo4j
```

Or directly with with the _docker stop_ command (if you provided the **CONTAINER_NAME** when starting, then you need to 
supply it again).

Note that either way, the container is deleted (due ot the use of the docker ```--rm``` flag during start up).
