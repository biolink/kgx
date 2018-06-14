## Command Line Usage
Use the `--help` flag with any command to view documentation. 

See the [Makefile](https://github.com/NCATS-Tangerine/kgx/blob/master/Makefile) for examples,

```
make examples
```

To run these examples use,
```
make run_examples
```

### Neo4j Upload
The `neo4j-upload` command takes any number of input files, builds a [networkx](https://networkx.github.io/) graph from them, and uploads that graph to a [neo4j](https://neo4j.com/) database. To do this it of course needs the database address, username, and password. This will only work through [bolt](https://neo4j.com/docs/operations-manual/current/configuration/connectors/). By default you can access a local neo4j instance at the address `bolt://localhost:7687`.
```
Usage: kgx neo4j-upload [OPTIONS] ADDRESS USERNAME PASSWORD INPUTS...
```
The `--input-type` option can be used to specify the format of these files: csv, ttl, json, txt, graphml, rq, tsv.

### Neo4j Download
The `neo4j-download` command downloads a neo4j instance, builds a networkx graph from it, and saves it to the specified file. Like the upload command, this will only work through bolt.
```
Usage: kgx neo4j-download [OPTIONS] ADDRESS USERNAME PASSWORD OUTPUT
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
Usage: kgx validate [OPTIONS] INPUTS...
```
The `--input-type` option can be used to specify the format of these files: csv, ttl, json, txt, graphml, rq, tsv.

### Dump
The `dump` command takes any number of input file paths (all with the same file format), and outputs a file in the desired format.

```
Usage: kgx dump [OPTIONS] INPUTS... OUTPUT
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
