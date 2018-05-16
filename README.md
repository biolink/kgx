# knowledge graph interchange

A utility library and set of command line tools for exchanging data in knowledge graphs.

The tooling here is partly generic but intended primarily for building
the
[translator-knowledge-graph](https://github.com/NCATS-Tangerine/translator-knowledge-graph).

For additional background see the [Translator Knowledge Graph Drive](http://bit.ly/tr-kg)

## Installation
```
pip3 install -r requirements.txt
python3 setup.py install
```

The installation requires Python 3.

For convenience, make use of the `venv` module in Python 3 to create a lightweight virtual environment:
```
python3 -m venv env
source env/bin/activate

pip install -r requirements.txt
python setup.py install
```

## Command Line Usage
Use the `--help` flag to get help. Right now there is a single command:
```
Usage: kgx dump [OPTIONS] [INPUT]... OUTPUT

  Transforms a knowledge graph from one representation to another
  INPUT  : any number of files or endpoints
  OUTPUT : the output file

Options:
  --input-type TEXT   Extention type of input files: ttl, json, csv, rq, tsv,
                      graphml
  --output-type TEXT  Extention type of output files: ttl, json, csv, rq, tsv,
                      graphml
  --help              Show this message and exit.
```

CSV/TSV representation require two files, one that represents the vertex set and
one for the edge set. JSON, TTL, and GRAPHML files represent a whole graph in a
single file. For this reason when creating CSV/TSV representation we will zip
the resulting files in a .tar file.

The format will be inferred from the file extention. But if  this cannot be done
then the `--input-type` and `--output-type` flags are useful to tell the program
what formats to use. Currently not all conversions are supported.

Here are some examples that mirror the [tests](tests/):

```
$ kgx dump --output-type=csv tests/resources/x1n.csv tests/resources/x1e.csv target/x1out
File created at: target/x1out.tar
$ kgx dump tests/resources/x1n.csv tests/resources/x1e.csv target/x1n.graphml
File created at: target/x1n.graphml
$ kgx dump tests/resources/monarch/biogrid_test.ttl target/bgcopy.csv
File created at: target/bgcopy.csv.tar
$ kgx dump tests/resources/monarch/biogrid_test.ttl target/x1n.graphml
File created at: target/x1n.graphml
$ kgx dump tests/resources/monarch/biogrid_test.ttl target/x1n.json
File created at: target/x1n.json
```

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
